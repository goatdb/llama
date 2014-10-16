/*
 * delete_edges.h
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


#ifndef LL_TEST_DELETE_EDGES_H
#define LL_TEST_DELETE_EDGES_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <float.h>
#include <limits.h>
#include <cmath>
#include <algorithm>
#include <omp.h>

#include "llama/ll_writable_graph.h"
#include "benchmarks/benchmark.h"


/**
 * Test: Delete edges
 */
template <class Graph>
class ll_t_delete_edges : public ll_benchmark<Graph> {


public:

	/**
	 * Create the test
	 */
	ll_t_delete_edges() : ll_benchmark<Graph>("[Test] Delete Edges") {
	}


	/**
	 * Destroy the test
	 */
	virtual ~ll_t_delete_edges(void) {
	}


	/**
	 * Run the benchmark
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) {

		Graph& G = *this->_graph;

		printf("\nDELETE EDGES TEST START\n");
		printf(" * Delete: "); fflush(stdout);

		int num_nodes = 0;
		int num_edges = 0;
		int numDeletedEdges = 0;

		G.tx_begin();

#pragma omp parallel
		{
			int nn = 0;
			int ne = 0;
			int nd = 0;

#pragma omp for schedule(dynamic,4096)
			for (node_t n = 0; n < G.max_nodes(); n++) {
				nn++;

				ll_edge_iterator iter;
				G.out_iter_begin(iter, n);
				for (edge_t v_idx = G.out_iter_next(iter); v_idx != LL_NIL_EDGE;
						v_idx = G.out_iter_next(iter)) {
					node_t v = LL_ITER_OUT_NEXT_NODE(G, iter, v_idx);
					(void) v;
					ne++;

					if (v_idx % 10 == 0) {
						G.delete_edge(n, v_idx);
						//fprintf(stderr, "Delete %08lx %08ld --> %08ld\n", v_idx, n, v);
						nd++;
					}
				}
			}

			ATOMIC_ADD(&num_nodes, nn);
			ATOMIC_ADD(&num_edges, ne);
			ATOMIC_ADD(&numDeletedEdges, nd);
		}
		//printf("\n     --> ");

		printf("%d edges originaly, %d deleted, %d left\n",
				num_edges, numDeletedEdges, num_edges - numDeletedEdges); fflush(stdout);

		G.tx_commit();

		printf(" * Validate: "); fflush(stdout);

		int num_edges2_count = 0;
		int num_edges2_degree = 0;
		bool ok = true;
		bool ok_edge_sources = true;

		G.tx_begin();

#pragma omp parallel
		{
			int ne = 0;
			int ng = 0;

#pragma omp for schedule(dynamic,4096)
			for (node_t n = 0; n < G.max_nodes(); n++) {

				ll_edge_iterator iter;
				G.out_iter_begin(iter, n);
				for (edge_t v_idx = G.out_iter_next(iter); v_idx != LL_NIL_EDGE;
						v_idx = G.out_iter_next(iter)) {

					ne++;

					if (v_idx % 10 == 0) {
						ok = false;
					}
				}

				assert(G.out_degree(n) >= 0);
				ng += G.out_degree(n);
			}

			ATOMIC_ADD(&num_edges2_count, ne);
			ATOMIC_ADD(&num_edges2_degree, ng);
		}
		//printf("\n     --> ");

		printf("%d counted, %d by summing out_degree\n",
				num_edges2_count, num_edges2_degree); fflush(stdout);

		G.tx_commit();

		if (!ok || num_edges2_count != num_edges2_degree || num_edges2_count != num_edges - numDeletedEdges) {
			printf("     --> failed");
			if (!ok_edge_sources) printf(" [edge sources do not match]");
			printf("\n");
			return NAN;
		}

		printf(" * Checkpoint: "); fflush(stdout);

		G.checkpoint();

		printf("done\n"); fflush(stdout);

		printf(" * Validate: "); fflush(stdout);

		int num_edges3_count = 0;
		int num_edges3_degree = 0;
		ok = true;

		G.tx_begin();

#pragma omp parallel
		{
			int ne = 0;
			int ng = 0;

#pragma omp for schedule(dynamic,4096)
			for (node_t n = 0; n < G.max_nodes(); n++) {

				ll_edge_iterator iter;
				G.out_iter_begin(iter, n);
				for (edge_t v_idx = G.out_iter_next(iter); v_idx != LL_NIL_EDGE;
						v_idx = G.out_iter_next(iter)) {

					ne++;
				}

				ng += G.out_degree(n);
			}

			ATOMIC_ADD(&num_edges3_count, ne);
			ATOMIC_ADD(&num_edges3_degree, ng);
		}
		//printf("\n     --> ");

		printf("%d counted, %d by summing out_degree\n",
				num_edges2_count, num_edges2_degree); fflush(stdout);

		G.tx_commit();

		if (!ok || num_edges2_count != num_edges2_degree || num_edges2_count != num_edges - numDeletedEdges) {
			printf("     --> failed");
			if (!ok_edge_sources) printf(" [edge sources do not match]");
			printf("\n");
			return NAN;
		}

		printf("DID NOT CRASH :)\n");
		return NAN;
	}
};

#endif
