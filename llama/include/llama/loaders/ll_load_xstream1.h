/*
 * ll_load_xstream1.h
 * LLAMA Graph Analytics
 *
 * Copyright 2014
 *      The President and Fellows of Harvard College.
 *
 * Copyright 2014
 *      Oracle Labs.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE UNIVERSITY AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE UNIVERSITY OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */


#ifndef LL_LOAD_XSTREAM1_H_
#define LL_LOAD_XSTREAM1_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <cstdio>
#include <sstream>
#include <unistd.h>

#include "llama/ll_mem_array.h"
#include "llama/ll_writable_graph.h"

#include "llama/loaders/ll_load_async_writable.h"
#include "llama/loaders/ll_load_utils.h"


/**
 * The X-Stream Type 1 file loader
 */
class ll_loader_xs1 : public ll_file_loader {

public:

	/**
	 * Create a new instance of ll_loader_xs1
	 */
	ll_loader_xs1() {}


	/**
	 * Destroy the instance
	 */
	virtual ~ll_loader_xs1() {}


	/**
	 * Determine if this file can be opened by this loader
	 *
	 * @param file the file
	 * @return true if it can be opened
	 */
	virtual bool accepts(const char* file) {

		return strcmp(ll_file_extension(file), "dat") == 0
			|| strcmp(ll_file_extension(file), "xs1") == 0;
	}


	/**
	 * Load directly into the read-only representation by creating a new
	 * level
	 *
	 * @param graph the graph
	 * @param file the file
	 * @param config the loader configuration
	 */
	virtual void load_direct(ll_mlcsr_ro_graph* graph, const char* file,
			const ll_loader_config* config) {

		ll_loader_config c;
		if (config != NULL) c = *config;
		c.lc_partial_load_part = 0;
		c.lc_partial_load_num_parts = 0;

		xs1_loader loader(file, config);

		bool r = loader.load_direct(graph, &c);
		if (!r) abort();
	}


	/**
	 * Load incrementally into the writable representation
	 *
	 * @param graph the graph
	 * @param file the file
	 * @param config the loader configuration
	 */
	virtual void load_incremental(ll_writable_graph* graph, const char* file,
			const ll_loader_config* config) {

		ll_loader_config c;
		if (config != NULL) c = *config;
		c.lc_partial_load_part = 0;
		c.lc_partial_load_num_parts = 0;

		xs1_loader loader(file, config);

		bool r = loader.load_incremental(graph, &c);
		if (!r) abort();
	}


	/**
	 * Create a data source object for the given file
	 *
	 * @param file the file
	 * @return the data source
	 */
	virtual ll_data_source* create_data_source(const char* file) {
		return new xs1_loader(file);
	}


private:

	/**
	 * Item format for external sort - for out-edges
	 */
	struct xs1 {

		unsigned tail;
		unsigned head;
		float weight;
	};


	/**
	 * Get next line from the .net file
	 *
	 * @param fin the file
	 * @param p the output pointer
	 * @return true if it was okay, false on EOF
	 */
	static bool xs1_next(FILE* fin, xs1* p) {

		ssize_t read = fread(p, sizeof(*p), 1, fin);
		if (read < 0) {
			perror("read");
			abort();
		}

		return read > 0;
	}


	/**
	 * The direct loader for X-Stream Type 1 files
	 */
	class xs1_loader : public ll_edge_list_loader<unsigned,
		true, float, LL_T_FLOAT>
	{	

		std::string _file_name;
		FILE* _file;

		bool _has_stats;
		size_t _nodes;
		size_t _edges;

		ll_loader_config _config;

		off_t _start_offset;
		size_t _edges_loaded;


	public:

		/**
		 * Create an instance of class xs1_loader
		 *
		 * @param file_name the file name
		 * @param config the loader config
		 */
		xs1_loader(const char* file_name, const ll_loader_config* config = NULL)
			: ll_edge_list_loader<unsigned, true, float, LL_T_FLOAT>() {

			if (config != NULL) {
				_config = *config;

				if (_config.lc_partial_load_num_parts > 0) {
					if (_config.lc_partial_load_part <= 0
							|| (_config.lc_partial_load_part
								> _config.lc_partial_load_num_parts)) {
						LL_E_PRINT("The partial load part ID is out of bounds\n");
						abort();
					}
				}
			}

			_file_name = file_name;
			int f = open(file_name, O_RDONLY);
			if (f < 0) {
				perror("Cannot open the input file");
				abort();
			}

			struct stat st;
			if (fstat(f, &st) != 0) {
				perror("Cannot stat the input file");
				abort();
			}

			_file = fdopen(f, "rb");
			if (_file == NULL) {
				perror("Cannot open the input stream");
				abort();
			}


			// Stats

			_nodes = 0;
			_edges = 0;
			_has_stats = false;

			FILE* f_ini = fopen((_file_name + ".ini").c_str(), "r");
			if (f_ini == NULL) {
				LL_W_PRINT("X-Stream Type 1 INI file not found: %s\n",
						(_file_name + ".ini").c_str());
			}
			else {
				char* line = NULL;
				size_t len = 0;
				ssize_t read;

				while ((read = getline(&line, &len, f_ini)) >= 0) {
					if (strncmp(line, "vertices=", 9) == 0) {
						char* p = strchr(line, '=') + 1;
						while (*p != '\0' && isspace(*p)) p++;
						_nodes = atol(p);
					}
					if (strncmp(line, "edges=", 6) == 0) {
						char* p = strchr(line, '=') + 1;
						while (*p != '\0' && isspace(*p)) p++;
						_edges = atol(p);
					}
				}

				_has_stats = _nodes > 0 && _edges > 0;

				if (line) free(line);
				fclose(f_ini);
			}

			if (_edges <= 0) {
				_edges = st.st_size / sizeof(xs1);
			}


			// Start/stop offsets

			_edges_loaded = 0;

			if (_config.lc_partial_load_num_parts > 0) {
				size_t file_edges = _edges;
				_start_offset = (file_edges * (_config.lc_partial_load_part - 1)
					/ _config.lc_partial_load_num_parts) * sizeof(xs1);
				_edges = (file_edges * _config.lc_partial_load_part
						/ _config.lc_partial_load_num_parts)
					- (_start_offset / sizeof(xs1));
				rewind();
			}
			else {
				_start_offset = 0;
			}
		}


		/**
		 * Destroy the loader
		 */
		virtual ~xs1_loader() {
			if (_file != NULL) fclose(_file);
		}


	protected:

		/**
		 * Read the next edge
		 *
		 * @param o_tail the output for tail
		 * @param o_head the output for head
		 * @param o_weight the output for weight (ignore if HasWeight is false)
		 * @return true if the edge was loaded, false if EOF or error
		 */
		virtual bool next_edge(unsigned* o_tail, unsigned* o_head,
				float* o_weight) {

			if (_edges_loaded >= _edges) return false;

			xs1 e;
			bool b = xs1_next(_file, &e);
			if (!b) return false;

			_edges_loaded++;

			*o_tail = e.tail;
			*o_head = e.head;
			*o_weight = e.weight;

			return true;
		}


		/**
		 * Rewind the input file
		 */
		virtual void rewind() {
			fseek(_file, _start_offset, SEEK_SET);
			_edges_loaded = 0;
		}


		/**
		 * Get graph stats if they are available
		 *
		 * @param o_nodes the output for the number of nodes (1 + max node ID)
		 * @param o_edges the output for the number of edges
		 * @return true if succeeded, or false if not or the info is not available
		 */
		virtual bool stat(size_t* o_nodes, size_t* o_edges) {

			if (_has_stats) {
				*o_nodes = _nodes;
				*o_edges = _edges;
				return true;
			}
			else {
				return false;
			}
		}
	};
};

#endif
