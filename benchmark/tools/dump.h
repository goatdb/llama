/*
 * dump.h
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


#ifndef LL_DUMP_H
#define LL_DUMP_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <float.h>
#include <limits.h>
#include <cmath>
#include <algorithm>
#include <omp.h>

#include "benchmarks/benchmark.h"


/**
 * Tool: Dump the graph
 */
template <class Graph>
class ll_t_dump : public ll_benchmark<Graph> {

	FILE* _out;


public:

	/**
	 * Create the tool
	 */
	ll_t_dump() : ll_benchmark<Graph>("[Tool] Dump") {
		_out = stdout;
	}


	/**
	 * Destroy the tool
	 */
	virtual ~ll_t_dump(void) {
	}


	/**
	 * Run the tool
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) {

		Graph& G = *this->_graph;
		size_t max_nodes = G.max_nodes();
		bool reverse = G.has_reverse_edges();

		printf("\n\n");

		fprintf(_out, "Out-edges\n");
		for (size_t n = 0; n < max_nodes; n++) {
			fprintf(_out, "%5ld:", n);

			ll_edge_iterator iter;
			G.out_iter_begin(iter, n);
			for (edge_t v_idx = G.out_iter_next(iter);
					v_idx != LL_NIL_EDGE;
					v_idx = G.out_iter_next(iter)) {
				fprintf(_out, "\t%ld", iter.last_node);
			}
			fprintf(_out, "\n");
		}

		if (reverse) {
			fprintf(_out, "\nIn-edges\n");
			for (size_t n = 0; n < max_nodes; n++) {
				fprintf(_out, "%5ld:", n);

				ll_edge_iterator iter;
				G.inm_iter_begin(iter, n);
				for (node_t v = G.inm_iter_next(iter);
						v != LL_NIL_NODE;
						v = G.inm_iter_next(iter)) {
					fprintf(_out, "\t%ld", v);
				}
				fprintf(_out, "\n");
			}
		}

		fprintf(_out, "\n");

        return NAN;
    }


	/**
	 * Print the results
	 */
	virtual void print_results(void) {
    }
};

#endif
