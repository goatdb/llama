/*
 * delete_nodes.h
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


#ifndef LL_TEST_DELETE_NODES_H
#define LL_TEST_DELETE_NODES_H

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
 * Test: Delete nodes
 */
template <class Graph>
class ll_t_delete_nodes : public ll_benchmark<Graph> {


public:

	/**
	 * Create the test
	 */
	ll_t_delete_nodes()
		: ll_benchmark<Graph>("[Test] Delete Nodes") {
	}


	/**
	 * Destroy the test
	 */
	virtual ~ll_t_delete_nodes(void) {
	}


	/**
	 * Run the benchmark
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) {

		Graph& G = *this->_graph;

		printf("\nDELETE NODES TEST START\n");
		printf(" * Delete: "); fflush(stdout);

		int num_nodes = 0;
		int numDeletedNodes = 0;

		G.tx_begin();

#pragma omp parallel
		{
			int nn = 0;
			int nd = 0;

#pragma omp for schedule(dynamic,4096)
			for (node_t n = 0; n < G.max_nodes(); n++) {
				nn++;

				if (n % 10 == 0) {
					G.delete_node(n);
					nd++;
				}
			}

			ATOMIC_ADD(&num_nodes, nn);
			ATOMIC_ADD(&numDeletedNodes, nd);
		}
		//printf("\n     --> ");

		printf("%d nodes originaly, %d deleted, %d left\n",
				num_nodes, numDeletedNodes, num_nodes - numDeletedNodes); fflush(stdout);

		G.tx_commit();

		printf(" * Validate: "); fflush(stdout);

		int num_nodes2_count = 0;
		int num_nodes2_error = 0;

		bool ok_d_odeg  = true;
		bool ok_d_ideg  = true;
		bool ok_d_oiter = true;
		bool ok_d_iiter = true;
		bool ok_f_odeg  = true;
		bool ok_f_ideg  = true;
		bool ok_f_oiter = true;
		bool ok_f_iiter = true;

		node_t n_f_oiter = -1;
		node_t n_f_iiter = -1;
		node_t n_f_odeg = -1;
		node_t n_f_ideg = -1;

		G.tx_begin();

#pragma omp parallel
		{
			int nn = 0;
			int ne = 0;

#pragma omp for schedule(dynamic,4096)
			for (node_t n = 0; n < G.max_nodes(); n++) {

				if (n % 10 == 0) {

					if (G.out_degree(n) != 0) ok_d_odeg = false;
					if (G.in_degree(n)  != 0) ok_d_ideg = false;

					ll_edge_iterator iter;

					G.out_iter_begin(iter, n);
					if (G.out_iter_next(iter) != LL_NIL_EDGE) ok_d_oiter = false;

					G.inm_iter_begin(iter, n);
					if (G.inm_iter_next(iter) != LL_NIL_EDGE) ok_d_iiter = false;
				}
				else {
					size_t x = 0;
					ll_edge_iterator iter;

					G.out_iter_begin(iter, n);
					for (edge_t v_idx = G.out_iter_next(iter); v_idx != LL_NIL_EDGE;
							v_idx = G.out_iter_next(iter)) {
						node_t m = LL_ITER_OUT_NEXT_NODE(G, iter, v_idx);

						if (m % 10 == 0) {
							ne++;
							ok_f_oiter = false;
							n_f_oiter = n;
						}

						x++;
					}

					if (x != G.out_degree(n)) {
						ok_f_odeg = false;
						n_f_odeg = n;
					}

					x = 0;
					G.inm_iter_begin(iter, n);
					for (node_t m = G.inm_iter_next(iter); m != LL_NIL_NODE;
							m = G.inm_iter_next(iter)) {

						if (m % 10 == 0) {
							ne++;
							ok_f_iiter = false;
							n_f_iiter = n;
						}

						x++;
					}

					if (x != G.in_degree(n)) {
						ok_f_ideg = false;
						n_f_ideg = n;
					}
				}
			}

			ATOMIC_ADD(&num_nodes2_count, nn);
			ATOMIC_ADD(&num_nodes2_error, ne);
		}

		printf("deleted nodes [degrees %s/%s, iterators %s/%s], other nodes [degrees %s/%s, iterators %s/%s]\n",
				ok_d_odeg  ? "ok" : "FAILED",
				ok_d_ideg  ? "ok" : "FAILED",
				ok_d_oiter ? "ok" : "FAILED",
				ok_d_iiter ? "ok" : "FAILED",
				ok_f_odeg  ? "ok" : "FAILED",
				ok_f_ideg  ? "ok" : "FAILED",
				ok_f_oiter ? "ok" : "FAILED",
				ok_f_iiter ? "ok" : "FAILED"
			  );
		fflush(stdout);

		G.tx_commit();

		if (!ok_d_odeg || !ok_d_oiter
				|| !ok_d_ideg || !ok_d_iiter
				|| !ok_f_odeg || !ok_f_oiter
				|| !ok_f_ideg || !ok_f_iiter
		   ) {
			printf("     --> failed\n");

			if (n_f_oiter >= 0) {
				printf("Iterators-out:\n");
				printf("%8ld [out]:", n_f_oiter); print_exp_adj_out(G, n_f_oiter);
				//printf("%8ld [ in]:", n_f_oiter); print_exp_adj_in(G, n_f_oiter);
			}

			if (n_f_iiter >= 0) {
				printf("Iterators-in:\n");
				//printf("%8ld [out]:", n_f_iiter); print_exp_adj_out(G, n_f_iiter);
				printf("%8ld [ in]:", n_f_iiter); print_exp_adj_in(G, n_f_iiter);
			}

			if (n_f_odeg >= 0) {
				printf("Degrees-out:\n");
				printf("%8ld [out]:", n_f_odeg); print_exp_adj_out(G, n_f_odeg);
			}

			if (n_f_ideg >= 0) {
				printf("Degrees-in:\n");
				printf("%8ld [ in]:", n_f_ideg); print_exp_adj_in(G, n_f_ideg);
			}

			return NAN;
		}

		printf(" * Checkpoint: "); fflush(stdout);

		G.checkpoint();

		printf("done\n"); fflush(stdout);

		printf(" * Validate: "); fflush(stdout);

		G.tx_begin();

		//#pragma omp parallel
		{
			int nn = 0;
			int ne = 0;

			//#pragma omp for schedule(dynamic,4096)
			for (node_t n = 0; n < G.max_nodes(); n++) {

				if (n % 10 == 0) {

					if (G.out_degree(n) != 0) ok_d_odeg = false;
					if (G.in_degree(n)  != 0) ok_d_ideg = false;

					ll_edge_iterator iter;

					G.out_iter_begin(iter, n);
					if (G.out_iter_next(iter) != LL_NIL_EDGE) ok_d_oiter = false;
					if (!ok_d_oiter) {
						printf("Error: %8ld [out]:", n); print_exp_adj_out(G, n);
						return NAN;
					}

					G.inm_iter_begin(iter, n);
					if (G.inm_iter_next(iter) != LL_NIL_EDGE) ok_d_iiter = false;
				}
				else {
					size_t x = 0;
					ll_edge_iterator iter;

					G.out_iter_begin(iter, n);
					for (edge_t v_idx = G.out_iter_next(iter); v_idx != LL_NIL_EDGE;
							v_idx = G.out_iter_next(iter)) {
						node_t m = LL_ITER_OUT_NEXT_NODE(G, iter, v_idx);

						if (m % 10 == 0) {
							ne++;
							ok_f_oiter = false;
							n_f_oiter = n;
						}

						x++;
					}

					if (x != G.out_degree(n)) {
						ok_f_odeg = false;
						n_f_odeg = n;
					}

					x = 0;
					G.inm_iter_begin(iter, n);
					for (node_t m = G.inm_iter_next(iter); m != LL_NIL_NODE;
							m = G.inm_iter_next(iter)) {

						if (m % 10 == 0) {
							ne++;
							ok_f_iiter = false;
							n_f_iiter = n;
						}

						x++;
					}

					if (x != G.in_degree(n)) {
						ok_f_ideg = false;
						n_f_ideg = n;
					}
				}
			}

			ATOMIC_ADD(&num_nodes2_count, nn);
			ATOMIC_ADD(&num_nodes2_error, ne);
		}

		printf("deleted nodes [degrees %s/%s, iterators %s/%s], other nodes [degrees %s/%s, iterators %s/%s]\n",
				ok_d_odeg  ? "ok" : "FAILED",
				ok_d_ideg  ? "ok" : "FAILED",
				ok_d_oiter ? "ok" : "FAILED",
				ok_d_iiter ? "ok" : "FAILED",
				ok_f_odeg  ? "ok" : "FAILED",
				ok_f_ideg  ? "ok" : "FAILED",
				ok_f_oiter ? "ok" : "FAILED",
				ok_f_iiter ? "ok" : "FAILED"
			  );
		fflush(stdout);

		G.tx_commit();

		if (!ok_d_odeg || !ok_d_oiter
				|| !ok_d_ideg || !ok_d_iiter
				|| !ok_f_odeg || !ok_f_oiter
				|| !ok_f_ideg || !ok_f_iiter
		   ) {
			printf("     --> failed\n");

			if (n_f_oiter >= 0) {
				printf("Iterators-out:\n");
				printf("%8ld [out]:", n_f_oiter); print_exp_adj_out(G, n_f_oiter);
				//printf("%8ld [ in]:", n_f_oiter); print_exp_adj_in(G, n_f_oiter);
			}

			if (n_f_iiter >= 0) {
				printf("Iterators-in:\n");
				//printf("%8ld [out]:", n_f_iiter); print_exp_adj_out(G, n_f_iiter);
				printf("%8ld [ in]:", n_f_iiter); print_exp_adj_in(G, n_f_iiter);
			}

			if (n_f_odeg >= 0) {
				printf("Degrees-out:\n");
				printf("%8ld [out]:", n_f_odeg); print_exp_adj_out(G, n_f_odeg);
			}

			if (n_f_ideg >= 0) {
				printf("Degrees-in:\n");
				printf("%8ld [ in]:", n_f_ideg); print_exp_adj_in(G, n_f_ideg);
			}

			return NAN;
		}

		printf("DID NOT CRASH :)\n");
		return NAN;
	}
};

#endif
