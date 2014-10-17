/*
 * ll_gen_rmat.h
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


#ifndef LL_GEN_RMAT_H_
#define LL_GEN_RMAT_H_

#include <cmath>
#include <sstream>
#include <string>
#include <vector>

#include "llama/ll_mem_array.h"
#include "llama/ll_writable_graph.h"

#include "llama/loaders/ll_load_async_writable.h"
#include "llama/loaders/ll_load_utils.h"


/**
 * R-MAT graph generator
 */
class ll_generator_rmat : public ll_file_loader {

public:

	/**
	 * Create a new instance of ll_generator_rmat
	 */
	ll_generator_rmat() {}


	/**
	 * Destroy the instance
	 */
	virtual ~ll_generator_rmat() {}


	/**
	 * Determine if this "file" can be opened by this loader
	 *
	 * @param file the file
	 * @return true if it can be opened
	 */
	virtual bool accepts(const char* file) {

		if (*file == '\0') return false;
		if (file[strlen(file)-1] != ')') return false;

		return strncasecmp(file, "r-mat(", 6) == 0
			|| strncasecmp(file, "rmat(", 5) == 0;
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

		rmat_generator loader(file);
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

		rmat_generator loader(file);
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
		return new rmat_generator(file);
	}


private:

	/**
	 * The generator (no weights for now)
	 */
	class rmat_generator : public ll_edge_list_loader<unsigned, false>
	{	

		std::string _command_string;
		std::string _command;

		size_t _nodes;
		size_t _edges;

		size_t _scale;
		double _degree;
		double _a, _b, _c, _d;
		bool _noise;

		unsigned _seed;

		size_t _generated_edges;
		unsigned _state;


	public:

		/**
		 * Create an instance of class rmat_generator
		 *
		 * @param file_name the file name
		 */
		rmat_generator(const char* file_name)
			: ll_edge_list_loader<unsigned, false>() {

			_command_string = file_name;


			// Parse the string: RMAT(scale, degree [, a, b, c [, seed]])

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

			if (args.size() != 2 && args.size() != 5 && args.size() != 6) {
				LL_E_PRINT("Invalid syntax, expected: "
						"RMAT(scale, degree [, a, b, c [, seed]])\n");
				abort();
			}

			_scale = atol(args[0].c_str());
			_degree = atof(args[1].c_str());

			if (args.size() > 2) {
				_a = atof(args[2].c_str());
				_b = atof(args[3].c_str());
				_c = atof(args[4].c_str());
			}
			else {
				_a = 0.57;
				_b = 0.19;
				_c = 0.19;
			}
			
			if (args.size() > 5)
				_seed = atol(args[5].c_str());
			else
				_seed = time(NULL);

			if (_scale <= 0 || _scale > 31) {
				LL_E_PRINT("Invalid scale\n");
				abort();
			}

			if (_degree <= 0) {
				LL_E_PRINT("Invalid degree\n");
				abort();
			}

			_d = 1.0 - (_a + _b + _c);

			if (_a < 0 || _b < 0 || _c < 0 || _a + _b + _c > 1) {
				LL_E_PRINT("Invalid R-MAT probabilities\n");
				abort();
			}

			_noise = true;

			_nodes = (size_t) (0.1 + pow(2, _scale));
			_edges = (size_t) (0.1 + round(_degree * _nodes));

			LL_D_PRINT("Nodes = %lu, edges = %lu\n", _nodes, _edges);


			// Initialize the generator

			rewind();
		}


		/**
		 * Destroy the loader
		 */
		virtual ~rmat_generator() {
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

			double a = _a;
			double b = _b;
			double c = _c;
			double d = _d;

			size_t h = 0;
			size_t t = 0;
			size_t bit = 1ul << (_scale - 1);

			while (bit != 0) {

				double r = rand_r(&_state) / (double) RAND_MAX;
				if (r < a) {
				}
				else if (r < a + b) {
					h |= bit;
				}
				else if (r < a + b + c) {
					t |= bit;
				}
				else {
					t |= bit;
					h |= bit;
				}

				bit >>= 1;

				if (_noise) {

					a *= 0.95 + 0.1 * (rand_r(&_state) / (double) RAND_MAX);
					b *= 0.95 + 0.1 * (rand_r(&_state) / (double) RAND_MAX);
					c *= 0.95 + 0.1 * (rand_r(&_state) / (double) RAND_MAX);
					d *= 0.95 + 0.1 * (rand_r(&_state) / (double) RAND_MAX);

					double n = 1.0 / (a + b + c + d);
					a *= n;
					b *= n;
					c *= n;
					d = 1.0 - (a + b + c);
				}
			}

			assert((size_t) h < _nodes);
			assert((size_t) t < _nodes);

			*o_tail = h;
			*o_head = t;

			LL_D_NODE2_PRINT(t, h, "%lu --> %lu\n", t, h);

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
			*o_edges = _edges;
			return true;
		}
	};
};

#endif
