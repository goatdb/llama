/*
 * flatten.h
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


#ifndef LL_FLATTEN_H
#define LL_FLATTEN_H

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
 * Tool: Flatten the entire database into one level -- i.e. do a full merge
 */
template <class Graph>
class ll_t_flatten : public ll_benchmark<Graph> {

	std::string _out;


public:

	/**
	 * Create the tool
	 *
	 * @param out the output directory
	 */
	ll_t_flatten(const char* out) : ll_benchmark<Graph>("[Tool] Flatten") {
		_out = out;
	}


	/**
	 * Destroy the tool
	 */
	virtual ~ll_t_flatten(void) {
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


		// Initialize

		degree_t* degrees_out
			= (degree_t*) malloc(sizeof(*degrees_out)*max_nodes);
		memset(degrees_out, 0, sizeof(*degrees_out) * max_nodes);

		degree_t* degrees_in = NULL;
		if (reverse) {
			degrees_in = (degree_t*) malloc(sizeof(*degrees_in) * max_nodes);
			memset(degrees_in, 0, sizeof(*degrees_in) * max_nodes);
		}


		// Create the new database

#ifdef LL_PERSISTENCE
		ll_persistent_storage* storage
			= new ll_persistent_storage(_out.c_str());
#endif

		ll_writable_graph* p_new_w_graph = new ll_writable_graph
			(NULL /* XXX for now */, IF_LL_PERSISTENCE(storage,) max_nodes);
		ll_mlcsr_ro_graph& new_r_graph = p_new_w_graph->ro_graph();


		// Determine the degree counts

		for (node_t n = 0; n < G.max_nodes(); n++) {
			degrees_out[n] = G.out_degree(n);
		}

		if (reverse) {
			for (node_t n = 0; n < G.max_nodes(); n++) {
				degrees_in[n] = G.in_degree(n);
			}
		}


		// Create the out-edges

		new_r_graph.out().init_level_from_degrees(max_nodes, degrees_out,NULL); 
		auto* vt = new_r_graph.out().vertex_table(new_r_graph.num_levels()-1);
		LL_ET<node_t>* et = new_r_graph.out().edge_table(
				new_r_graph.num_levels()-1);

#pragma omp parallel
		{
			std::vector<std::pair<node_t, edge_t>> v;

#pragma omp for nowait schedule(dynamic,1)
			for (node_t n1 = 0; n1 < G.max_nodes(); n1 += 1024 * 256) {
				node_t n2 = n1 + 1024 * 256;
				if (n2 > G.max_nodes()) n2 = G.max_nodes();

				edge_t e = LL_NIL_EDGE;
				while (n1 < n2 && (e = (*vt)[n1].adj_list_start) == LL_NIL_EDGE) n1++;
				if (e == LL_NIL_EDGE) continue;

				size_t index = LL_EDGE_INDEX(e);

				for (node_t n = n1; n < n2; n++) {

					v.clear();
					ll_edge_iterator iter;
					G.out_iter_begin(iter, n);
					for (edge_t v_idx = G.out_iter_next(iter);
							v_idx != LL_NIL_EDGE;
							v_idx = G.out_iter_next(iter)) {
						v.push_back(std::pair<node_t, edge_t>(iter.last_node,
									v_idx));
					}

					std::sort(v.begin(), v.end());

					for (size_t i = 0; i < v.size(); i++) {
						(*et)[index] = LL_VALUE_CREATE((node_t) v[i].first);

						// TODO Edge properties
						/*edge_t edge = LL_EDGE_CREATE(0, index);
						  prop_weight->cow_write(edge, e.weight);*/

						index++;
					}
				}
			}
		}


		// Create the in-edges
		
		// TODO


		// Merge node properties

		// TODO


		// Cleanup

		new_r_graph.finish_level_edges();
		
		delete p_new_w_graph;

#ifdef LL_PERSISTENCE
		delete storage;
#endif

		if (degrees_in  != NULL) free(degrees_in);
		if (degrees_out != NULL) free(degrees_out);

        return NAN;
    }


	/**
	 * Print the results
	 */
	virtual void print_results(void) {
    }
};

#endif
