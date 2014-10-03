/*
 * ll_load_net.h
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


#ifndef LL_LOAD_NET_H_
#define LL_LOAD_NET_H_

#include <endian.h>
#include <sstream>

#include "llama/ll_mem_array.h"
#include "llama/ll_writable_graph.h"

#include "llama/loaders/ll_load_async_writable.h"
#include "llama/loaders/ll_load_utils.h"


/**
 * The SNAP .net loader
 */
class ll_loader_net : public ll_file_loader {

public:

	/**
	 * Create a new instance of ll_loader_net
	 */
	ll_loader_net() {}


	/**
	 * Destroy the instance
	 */
	virtual ~ll_loader_net() {}


	/**
	 * Determine if this file can be opened by this loader
	 *
	 * @param file the file
	 * @return true if it can be opened
	 */
	virtual bool accepts(const char* file) {

		return strcmp(ll_file_extension(file), "net") == 0
			|| strcmp(ll_file_extension(file), "snap") == 0;
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

		net_loader loader(file);
		bool r = loader.load_direct(graph, config);
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

		net_loader loader(file);
		bool r = loader.load_incremental(graph, config);
		if (!r) abort();
	}


	/**
	 * Create a data source object for the given file
	 *
	 * @param file the file
	 * @return the data source
	 */
	virtual ll_data_source* create_data_source(const char* file) {
		return new net_loader(file);
	}


private:

	/**
	 * Get next line from the .net file
	 *
	 * @param fin the file
	 * @param p_line the pointer to a malloc-ed line buffer
	 * @param p_line_len the pointer to the line buffer size
	 * @param p_tail the tail out pointer
	 * @param p_head the head out pointer
	 * @return true if it was okay, false on EOF
	 */
	static bool net_next_line(FILE* fin, char** p_line, size_t* p_line_len,
			unsigned* p_tail, unsigned* p_head) {

		ssize_t read;

		while ((read = getline(p_line, p_line_len, fin)) != -1) {

			char* line = *p_line;

			if (*line == '\0' || *line == '#'
					|| *line == '\n' || *line == '\r') continue;

			size_t ln = strlen(line)-1;
			if (line[ln] == '\n' || line[ln] == '\r') line[ln] = '\0';

			if (!isdigit(*line)) {
				fprintf(stderr, "Invalid .net format on line \"%s\"\n", *p_line);
				abort();
			}

			char* l = line;
			while (isdigit(*l)) l++;
			if (*l == '\0') {
				fprintf(stderr, "Invalid .net format on line \"%s\"\n", *p_line);
				abort();
			}

			while (isspace(*l)) l++;
			if (*l == '\0' || !isdigit(*l)) {
				fprintf(stderr, "Invalid .net format on line \"%s\"\n", *p_line);
				abort();
			}

			unsigned tail = atoi(line);
			unsigned head = atoi(l);

			*p_tail = tail;
			*p_head = head;

			return true;
		}

		return false;
	}


	/**
	 * The direct loader for X-Stream Type 1 files
	 */
	class net_loader : public ll_edge_list_loader<unsigned, false>
	{	

		std::string _file_name;
		FILE* _file;

		size_t _line_n;
		char* _line;


	public:

		/**
		 * Create an instance of class net_loader
		 *
		 * @param file_name the file name
		 */
		net_loader(const char* file_name)
			: ll_edge_list_loader<unsigned, false>() {

			_file_name = file_name;
			_file = fopen(file_name, "rt");
			if (_file == NULL) {
				perror("Cannot open the input file");
				abort();
			}

			_line_n = 64;
			_line = (char*) malloc(_line_n);
		}


		/**
		 * Destroy the loader
		 */
		virtual ~net_loader() {
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

			return net_next_line(_file, &_line, &_line_n, o_tail, o_head);
		}


		/**
		 * Rewind the input file
		 */
		virtual void rewind() {
			std::rewind(_file);
		}

	};
};

#endif
