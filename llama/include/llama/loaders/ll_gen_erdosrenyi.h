/*
 * ll_gen_erdosrenyi.h
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


#ifndef LL_GEN_ERDOSRENYI_H_
#define LL_GEN_ERDOSRENYI_H_

#include <sstream>
#include <string>
#include <vector>

#include "llama/ll_mem_array.h"
#include "llama/ll_writable_graph.h"

#include "llama/loaders/ll_load_async_writable.h"
#include "llama/loaders/ll_load_utils.h"


/**
 * Erdos-Renyi graph generator
 */
class ll_generator_erdos_renyi : public ll_file_loader {

public:

	/**
	 * Create a new instance of ll_generator_erdos_renyi
	 */
	ll_generator_erdos_renyi() {}


	/**
	 * Destroy the instance
	 */
	virtual ~ll_generator_erdos_renyi() {}


	/**
	 * Determine if this "file" can be opened by this loader
	 *
	 * @param file the file
	 * @return true if it can be opened
	 */
	virtual bool accepts(const char* file) {

		if (*file == '\0') return false;
		if (file[strlen(file)-1] != ')') return false;

		return strncasecmp(file, "erdosrenyi(", 11) == 0
			|| strncasecmp(file, "er(", 3) == 0;
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

		er_generator loader(file);
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

		er_generator loader(file);
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
		return new er_generator(file);
	}


private:

	/**
	 * The generator (no weights for now)
	 */
	class er_generator : public ll_edge_list_loader<unsigned, false>
	{	

		std::string _command_string;

		std::string _command;
		size_t _nodes;
		size_t _edges;
		unsigned _seed;

		size_t _generated_edges;
		unsigned _state;


	public:

		/**
		 * Create an instance of class er_generator
		 *
		 * @param file_name the file name
		 */
		er_generator(const char* file_name)
			: ll_edge_list_loader<unsigned, false>() {

			_command_string = file_name;


			// Parse the string: ER(nodes, edges [, seed])

			std::vector<std::string> args;

			char _s[_command_string.length()+1];
			strcpy(_s, _command_string.c_str());
			if (_s[strlen(_s)-1] == ')') _s[strlen(_s)-1] = '\0';

			char* p = strchr(_s, '(');
			if (p == NULL) abort();		// It should not have been accepted
			*p = '\0'; p++;
			_command = _s;

			while (true) {
				char* e = strchr(p, ',');
				if (e != NULL) *e = '\0';

				while (*p != '\0' && isspace(*p)) p++;
				while (*p != '\0' && isspace(p[strlen(p)-1]))
					p[strlen(p)-1] = '\0';

				args.push_back(p);

				if (e != NULL) p = e + 1; else break;
			}


			// Arguments

			if (args.size() != 2 && args.size() != 3) {
				LL_E_PRINT("Invalid syntax, expected: ER(nodes, edges [, seed])\n");
				abort();
			}

			_nodes = atol(args[0].c_str());
			_edges = atol(args[1].c_str());
			
			if (args.size() > 2)
				_seed = atol(args[2].c_str());
			else
				_seed = 0;


			// Initialize the generator

			rewind();
		}


		/**
		 * Destroy the loader
		 */
		virtual ~er_generator() {
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

			if (_generated_edges >= _edges) return false;

			*o_tail = ll_rand64_positive_r(&_state) % _nodes;
			*o_head = ll_rand64_positive_r(&_state) % _nodes;

			_generated_edges++;
			return true;
		}


		/**
		 * Rewind the input file
		 */
		virtual void rewind() {
			_state = _seed;
			_generated_edges = 0;
		}


		/**
		 * Get graph stats if they are available
		 *
		 * @param o_nodes the output for the number of nodes (1 + max node ID)
		 * @param o_edges the output for the number of edges
		 * @return true if succeeded, or false if not or the info is not available
		 */
		bool stat(size_t* o_nodes, size_t* o_edges) {
			*o_nodes = _nodes;
			*o_nodes = _edges;
			return true;
		}
	};
};

#endif
