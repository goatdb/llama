/*
 * print.h
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


#ifndef LL_PRINT_H
#define LL_PRINT_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <float.h>
#include <limits.h>
#include <cmath>
#include <algorithm>
#include <omp.h>


template <class EXP_GRAPH>
void print_exp_adj_out(EXP_GRAPH& G, node_t n) {
	if (!G.node_exists(n)) {
		printf(" %7s\n", "--");
		return;
	}
	ll_edge_iterator iter;
	G.out_iter_begin(iter, n);
	FOREACH_OUTEDGE_ITER(v_idx, G, iter) {
		node_t v = LL_ITER_OUT_NEXT_NODE(G, iter, v_idx);
		printf(" %7u", (unsigned) v);
		//printf("[%07x]", (unsigned long) v_idx);
	}
	printf("  [Degree: %d]\n", (int) G.out_degree(n));
}


template <class EXP_GRAPH>
void print_exp_adj_out_sorted(EXP_GRAPH& G, node_t n) {
	if (!G.node_exists(n)) {
		printf(" %7s\n", "--");
		return;
	}
	ll_edge_iterator iter;
	G.out_iter_begin(iter, n);
	std::vector<node_t> nodes;
	FOREACH_OUTEDGE_ITER(v_idx, G, iter) {
		node_t v = LL_ITER_OUT_NEXT_NODE(G, iter, v_idx);
		nodes.push_back(v);
	}
	std::sort(nodes.begin(), nodes.end());
	for (size_t i = 0; i < nodes.size(); i++)
		printf(" %7u", (unsigned) nodes[i]);
	printf("  [Degree: %d]\n", (int) G.out_degree(n));
}


template <class EXP_GRAPH>
void print_exp_adj_in(EXP_GRAPH& G, node_t n) {
	if (!G.node_exists(n)) {
		printf(" %7s\n", "--");
		return;
	}
	ll_edge_iterator iter;
	G.inm_iter_begin(iter, n);
	FOREACH_INNODE_ITER(v, G, iter) {
		printf(" %7u", (unsigned) v);
	}
	printf("  [Degree: %d]\n", (int) G.in_degree(n));
}


template <class MLCSR>
void print_exp_adj_within_level(MLCSR& g, node_t n, int l) {
	if (!g.node_exists(n)) {
		printf(" %7s\n", "--");
		return;
	}
	ll_edge_iterator iter;
	g.iter_begin_within_level(iter, n, l);
	FOREACH_ITER_WITHIN_LEVEL(v_idx, g, iter) {
		node_t v = LL_ITER_OUT_NEXT_NODE(g, iter, v_idx);
		printf(" %7u", (unsigned) v);
	}
	if (g.max_nodes(l) > n) {
		printf("  [Degree: %d]\n", (int) g.degree(n, l));
	}
	else {
		printf("\n");
	}
}


template <class EXP_GRAPH>
void print_exp_graph_out(EXP_GRAPH& G) {

	for (node_t n = 0; n < G.max_nodes(); n++) {
		if (!G.node_exists(n)) continue;
		printf(" %7ld:", n);
		print_exp_adj_out(G, n);
	}
}


template <class EXP_GRAPH>
void print_exp_graph_out_sorted(EXP_GRAPH& G) {

	for (node_t n = 0; n < G.max_nodes(); n++) {
		if (!G.node_exists(n) || G.out_degree(n) == 0) continue;
		printf(" %7ld:", n);
		print_exp_adj_out_sorted(G, n);
	}
}

#endif
