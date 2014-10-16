/*
 * triangle_counting.h
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


#ifndef LL_TRIANGLE_COUNTING_H
#define LL_TRIANGLE_COUNTING_H

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
 * Triangle counting
 */
template <class Graph>
class ll_b_triangle_counting_org : public ll_benchmark<Graph> {

	int64_t _num_triangles;


public:

	/**
	 * Create the benchmark
	 */
	ll_b_triangle_counting_org()
		: ll_benchmark<Graph>("Triangle Counting") {
	}


	/**
	 * Destroy the benchmark
	 */
	virtual ~ll_b_triangle_counting_org(void) {
	}


	/**
	 * Run the benchmark
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) {

		Graph& G = *this->_graph;

		int64_t T = 0 ;
		int64_t num_k_processed = 0 ;
		this->progress_init(G.max_nodes());

#pragma omp parallel
		{
			int64_t T_prv = 0 ;

#pragma omp for nowait schedule(dynamic,4096)
			for (node_t u = 0; u < G.max_nodes(); u ++) 
			{
				ll_edge_iterator iter;
				G.out_iter_begin(iter, u);
				for (edge_t v_idx = G.out_iter_next(iter);
						v_idx != LL_NIL_EDGE;
						v_idx = G.out_iter_next(iter)) {
					node_t v = LL_ITER_OUT_NEXT_NODE(G, iter, v_idx);
					if (v > u)
					{
						// Iterate over Common neighbors
#ifdef TEST_CXX_ITER
						ll_common_neighbor_iter_cxx<Graph> w_I(G, u, v);
#else
						ll_common_neighbor_iter<Graph> w_I(G, u, v);
#endif
						for (node_t w = w_I.get_next();
								w != LL_NIL_NODE; w = w_I.get_next()) 
						{
							if (w > v)
							{
								T_prv = T_prv + 1 ;
							}
						}
					}
				}

				if ((u % 1000) == 0) {
					int64_t x = __sync_add_and_fetch(&num_k_processed, 1);
					if (x%10 == 0) this->progress_update(x * 1000);
				}
			}
			ATOMIC_ADD<int64_t>(&T, T_prv);
		}

		this->progress_clear();
		_num_triangles = T;
		return T; 
	}


	/**
	 * Print the results
	 * 
	 * @param f the output file
	 */
	virtual void print_results(FILE* f) {
		fprintf(f, "Number of triangles: %lld\n", (long long int) _num_triangles);
	}
};


/**
 * Triangle counting, ignoring the edge direction.
 * 
 * Assumes a directed graph that is not a multi-graph even when undirected.
 */
template <class Graph>
class ll_b_triangle_counting_LI : public ll_benchmark<Graph> {

	int64_t _num_triangles;


public:

	/**
	 * Create the benchmark
	 */
	ll_b_triangle_counting_LI()
		: ll_benchmark<Graph>("Triangle Counting") {
	}


	/**
	 * Destroy the benchmark
	 */
	virtual ~ll_b_triangle_counting_LI(void) {
	}


	/**
	 * Check the given pair of nodes
	 *
	 * @param u the first node
	 * @param v the second node, such that u < v
	 * @return the number of triangles
	 */
	size_t count_for(Graph& G, node_t u, node_t v) {

		size_t count = 0;

		ll_edge_iterator iter_u_out;
		G.out_iter_begin(iter_u_out, u);
		edge_t eo_u = G.out_iter_next(iter_u_out);

		ll_edge_iterator iter_v_out;
		G.out_iter_begin(iter_v_out, v);
		edge_t eo_v = G.out_iter_next(iter_v_out);

		ll_edge_iterator iter_u_in;
		G.inm_iter_begin(iter_u_in, u);
		node_t ni_u = G.inm_iter_next(iter_u_in);

		ll_edge_iterator iter_v_in;
		G.inm_iter_begin(iter_v_in, v);
		node_t ni_v = G.inm_iter_next(iter_v_in);

		while ((eo_u != LL_NIL_EDGE
					|| ni_u != LL_NIL_NODE)
				&& (eo_v != LL_NIL_EDGE
					|| ni_v != LL_NIL_NODE)) {

			node_t mu;
			if (eo_u == LL_NIL_EDGE) mu = ni_u;
			else if (ni_u == LL_NIL_NODE) mu = iter_u_out.last_node;
			else mu = std::min(iter_u_out.last_node, ni_u);

			node_t mv;
			if (eo_v == LL_NIL_EDGE) mv = ni_v;
			else if (ni_v == LL_NIL_NODE) mv = iter_v_out.last_node;
			else mv = std::min(iter_v_out.last_node, ni_v);

			if (mu < mv) {
				if (mu == iter_u_out.last_node) {
					eo_u = G.out_iter_next(iter_u_out);
				}
				if (mu == ni_u) {
					ni_u = G.inm_iter_next(iter_u_in);
				}
			}
			else if (mv < mu) {
				if (mv == iter_v_out.last_node) {
					eo_v = G.out_iter_next(iter_v_out);
				}
				if (mv == ni_v) {
					ni_v = G.inm_iter_next(iter_v_in);
				}
			}
			else {
				if (v < mv) {
					count++;
				}

				if (mu == iter_u_out.last_node) {
					eo_u = G.out_iter_next(iter_u_out);
				}
				if (mu == ni_u) {
					ni_u = G.inm_iter_next(iter_u_in);
				}
				if (mv == iter_v_out.last_node) {
					eo_v = G.out_iter_next(iter_v_out);
				}
				if (mv == ni_v) {
					ni_v = G.inm_iter_next(iter_v_in);
				}
			}
		}

		return count;
	}


	/**
	 * Run the benchmark
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) {

		Graph& G = *this->_graph;

		// TODO Enforce that the adjacency lists are sorted

		if (G.num_levels() != 1) {
			fprintf(stderr, "The graph must have exactly 1 level\n");
			abort();
		}

		if (!G.has_reverse_edges()) {
			fprintf(stderr, "The graph must have reverse edges\n");
			abort();
		}

		int64_t T = 0 ;
		int64_t num_k_processed = 0 ;
		this->progress_init(G.max_nodes());

#pragma omp parallel
		{
			int64_t T_prv = 0 ;

#pragma omp for nowait schedule(dynamic,4096)
			for (node_t u = 0; u < G.max_nodes(); u ++) {
				ll_edge_iterator iter;

				G.out_iter_begin(iter, u);
				//node_t x = -1;
				for (edge_t v_idx = G.out_iter_next(iter);
						v_idx != LL_NIL_EDGE;
						v_idx = G.out_iter_next(iter)) {
					node_t v = LL_ITER_OUT_NEXT_NODE(G, iter, v_idx);
					//if (x == v) continue;
					//x = v;
					if (u < v) T_prv += count_for(G, u, v);
				}

				G.inm_iter_begin(iter, u);
				//x = -1;
				for (node_t v = G.inm_iter_next(iter);
						v != LL_NIL_NODE;
						v = G.inm_iter_next(iter)) {
					//if (x == v) continue;
					//x = v;
					if (u < v) T_prv += count_for(G, u, v);
				}

				if ((u % 1000) == 0) {
					int64_t x = __sync_add_and_fetch(&num_k_processed, 1);
					if (x%10 == 0) this->progress_update(x*1000);
				}
			}

			ATOMIC_ADD<int64_t>(&T, T_prv);
		}

		this->progress_clear();
		_num_triangles = T;
		return T; 
	}


	/**
	 * Print the results
	 * 
	 * @param f the output file
	 */
	virtual void print_results(FILE* f) {
		fprintf(f, "Number of triangles: %lld\n", (long long int) _num_triangles);
	}
};


/**
 * Triangle counting for undirected, single-level graphs loaded with the -U
 * flag.
 */
template <class Graph>
class ll_b_triangle_counting_LU : public ll_benchmark<Graph> {

	int64_t _num_triangles;


public:

	/**
	 * Create the benchmark
	 */
	ll_b_triangle_counting_LU()
		: ll_benchmark<Graph>("Triangle Counting") {
	}


	/**
	 * Destroy the benchmark
	 */
	virtual ~ll_b_triangle_counting_LU(void) {
	}


	/**
	 * Check the given pair of nodes
	 *
	 * @param u the first node
	 * @param v the second node, such that u < v
	 * @return the number of triangles
	 */
	size_t count_for(Graph& G, node_t u, node_t v) {

		size_t count = 0;

		ll_edge_iterator iter_u_out;
		G.out_iter_begin(iter_u_out, u);
		edge_t eo_u = G.out_iter_next(iter_u_out);

		ll_edge_iterator iter_v_out;
		G.out_iter_begin(iter_v_out, v);
		edge_t eo_v = G.out_iter_next(iter_v_out);

		while ((eo_u != LL_NIL_EDGE)
				&& (eo_v != LL_NIL_EDGE)) {

			node_t mu = iter_u_out.last_node;
			node_t mv = iter_v_out.last_node;

			if (mu < mv) {
				eo_u = G.out_iter_next(iter_u_out);
			}
			else if (mv < mu) {
				eo_v = G.out_iter_next(iter_v_out);
			}
			else {
				if (v < mv) {
					count++;
				}

				eo_u = G.out_iter_next(iter_u_out);
				eo_v = G.out_iter_next(iter_v_out);
			}
		}

		return count;
	}


	/**
	 * Run the benchmark
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) {

		Graph& G = *this->_graph;

		// TODO Enforce that the adjacency lists are sorted

		if (G.num_levels() != 1) {
			fprintf(stderr, "The graph must have exactly 1 level\n");
			abort();
		}

		int64_t T = 0 ;
		int64_t num_k_processed = 0 ;
		this->progress_init(G.max_nodes());

#pragma omp parallel
		{
			int64_t T_prv = 0 ;

#pragma omp for nowait schedule(dynamic,4096)
			for (node_t u = 0; u < G.max_nodes(); u ++) {
				ll_edge_iterator iter;

				G.out_iter_begin(iter, u);
				for (edge_t v_idx = G.out_iter_next(iter);
						v_idx != LL_NIL_EDGE;
						v_idx = G.out_iter_next(iter)) {
					node_t v = LL_ITER_OUT_NEXT_NODE(G, iter, v_idx);
					if (u < v) T_prv += count_for(G, u, v);
				}

				if ((u % 1000) == 0) {
					int64_t x = __sync_add_and_fetch(&num_k_processed, 1);
					if (x%10 == 0) this->progress_update(x*1000);
				}
			}

			ATOMIC_ADD<int64_t>(&T, T_prv);
		}

		this->progress_clear();
		_num_triangles = T;
		return T; 
	}


	/**
	 * Print the results
	 * 
	 * @param f the output file
	 */
	virtual void print_results(FILE* f) {
		fprintf(f, "Number of triangles: %lld\n", (long long int) _num_triangles);
	}
};


/**
 * Linear search in an adjacency list
 *
 * @param data the data
 * @param length the length
 * @param target the target node to find
 * @return true if there is an occurence
 */
inline bool findadj_linear(node_t* data, size_t length, node_t target) {
    for (int i = 0; i < (int) length && data[i] <= target; i++) {
        if (data[i] == target) return true;
    }
    return false;
}


/**
 * Binary search in an adjacency list
 *
 * @param data the data
 * @param length the length
 * @param target the target node to find
 * @return true if there is an occurence
 */
inline bool findadj(node_t* data, size_t length, node_t target) {

    if (length < 32)
		return findadj_linear(data, length, target);
	
    register size_t l = 0;
    register size_t h = length;
    register size_t m = l + (h-l)/2;

    while (h > l) {
        node_t n = data[m];
        if (target == n) return true;
        if (target > n)
            l = m + 1;
        else
            h = m;
        m = l + (h-l)/2;
    }

    return false;
}


/**
 * Count triangles in two adjacency lists
 *
 * @param u the node u
 * @param v the node v, assuming u < v
 * @param u_adj the adjacency list
 * @param u_num the length of the adjacency list
 * @param v_adj the adjacency list
 * @param v_num the length of the adjacency list
 * @return the number of common points > v found
 */
size_t count_triangles(node_t u, node_t v, node_t* u_adj, size_t u_num,
		node_t* v_adj, size_t v_num) {

	size_t r = 0;

	if (u_num < 32 * v_num) {
		int ui = 0;
		int vi = 0;
		while (ui < (node_t) u_num && vi < (node_t) v_num) {
			node_t du = u_adj[ui];
			node_t dv = v_adj[vi];
			if (du == dv) {
				r += dv > v;
				ui++;
				vi++;
			}
			else {
				ui += du < dv;
				vi += du > dv;
			}
		}
	}
	else {
		for (int i = 0; i < (node_t) v_num; i++) {
			node_t d = v_adj[i];
			if (d > v)
				r += findadj(u_adj, u_num, d);
		}
		/*for (int i = 0; i < u_num; i++) {
			node_t d = u_adj[i];
			if (d > v)
				r += findadj(v_adj, v_num, d);
		}*/
	}

	return r;
}


/**
 * Triangle counting for undirected, single-level graphs loaded with the -OD
 * flags.
 */
template <class Graph>
class ll_b_triangle_counting_LOD_org : public ll_benchmark<Graph> {

	int64_t _num_triangles;


public:

	/**
	 * Create the benchmark
	 */
	ll_b_triangle_counting_LOD_org()
		: ll_benchmark<Graph>("Triangle Counting") {
	}


	/**
	 * Destroy the benchmark
	 */
	virtual ~ll_b_triangle_counting_LOD_org(void) {
	}


	/**
	 * Run the benchmark
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) {

		Graph& G = *this->_graph;

		// TODO Enforce that the adjacency lists are sorted

		if (G.num_levels() != 1) {
			fprintf(stderr, "The graph must have exactly 1 level\n");
			abort();
		}

		int64_t T = 0 ;
		int64_t num_k_processed = 0 ;
		this->progress_init(G.max_nodes());

#pragma omp parallel
		{
			int64_t T_prv = 0 ;

#pragma omp for nowait schedule(dynamic,4096)
			for (node_t u = 0; u < G.max_nodes(); u ++) {
				ll_edge_iterator iter;
				ll_edge_iterator iter2;
				iter2.ptr = NULL;

				G.out_iter_begin(iter, u);
				size_t u_num = iter.left;
				node_t* u_adj = (node_t*) iter.ptr;

				for ( ; u_num > 0; u_num--, u_adj++) {
					if (*u_adj <= u) continue;

					G.out_iter_begin(iter2, *u_adj);
					size_t v_num = iter2.left;
					node_t* v_adj = (node_t*) iter2.ptr;

					T_prv += count_triangles(u, *u_adj, u_adj, u_num,
							v_adj, v_num);
				}

				if ((u % 1000) == 0) {
					int64_t x = __sync_add_and_fetch(&num_k_processed, 1);
					if (x%10 == 0) this->progress_update(x*1000);
				}
			}

			ATOMIC_ADD<int64_t>(&T, T_prv);
		}

		this->progress_clear();
		_num_triangles = T;
		return T; 
	}


	/**
	 * Print the results
	 * 
	 * @param f the output file
	 */
	virtual void print_results(FILE* f) {
		fprintf(f, "Number of triangles: %lld\n", (long long int) _num_triangles);
	}
};


/**
 * Comparator for node_pair_t, specific for the triangle counting
 */
struct ll_tc_node_pair_comparator {
	bool operator() (const node_pair_t& a, const node_pair_t& b) {
		return a.tail < b.tail;
	}
};


/**
 * Triangle counting for undirected, single-level graphs loaded with the -OD
 * flags -- new version.
 */
template <class Graph>
class ll_b_triangle_counting_LOD : public ll_benchmark<Graph> {

	int64_t _num_triangles;


public:

	/**
	 * Create the benchmark
	 */
	ll_b_triangle_counting_LOD()
		: ll_benchmark<Graph>("Triangle Counting") {
	}


	/**
	 * Destroy the benchmark
	 */
	virtual ~ll_b_triangle_counting_LOD(void) {
	}


	/**
	 * Run the benchmark
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) {

		// XXX This does not work with slcsr, as the ptr is not used by the
		// iterator

		Graph& G = *this->_graph;

		if (G.num_levels() != 1) {
			fprintf(stderr, "The graph must have exactly 1 level\n");
			abort();
		}

		auto* et = G.out().edge_table(); (void) et;
		ll_tc_node_pair_comparator node_pair_comparator;

		int64_t T = 0 ;
		this->progress_init(G.max_nodes());

#pragma omp parallel
		{
			int64_t T_prv = 0 ;
			size_t u_step = 128;

			ll_edge_iterator iter;
			ll_edge_iterator iter2;
			iter.ptr = NULL;
			iter2.ptr = NULL;

			size_t eb_capacity = 1000000;
			size_t eb_size = 0;
			node_t* eb = (node_t*) malloc(sizeof(node_t) * eb_capacity * 2);

#pragma omp for nowait schedule(dynamic,20)
			for (node_t u_start = 0; u_start < G.max_nodes(); u_start += u_step) {
				node_t u_end = std::min<node_t>(G.max_nodes(), u_start + u_step);

				node_t* p_start = NULL;
				node_t* p_end = NULL;

				for (node_t u = u_start; u < u_end; u++) {
					G.out_iter_begin(iter, u);
					if (iter.left > 0) {
						p_start = (node_t*) iter.ptr;
						break;
					}
				}

				for (node_t u = u_end-1; u >= u_start; u--) {
					G.out_iter_begin(iter, u);
					if (iter.left > 0) {
						p_end = (node_t*) iter.ptr + iter.left;
						break;
					}
				}

				if (p_start == NULL) continue;

				eb_size = (((size_t) (char*) p_end)
						- ((size_t) (char*) p_start)) / sizeof(node_t);
				if (eb_size > eb_capacity) {
					eb = (node_t*) realloc(eb, sizeof(node_t) * eb_size * 2);
					eb_capacity = eb_size;
				}

				node_t* p = eb;
				for (node_t u = u_start; u < u_end; u++) {
					G.out_iter_begin(iter, u);
					node_t* x = (node_t*) iter.ptr;
					for ( ; iter.left > 0; iter.left--, x++) {
						*(p++) = *x;
						*(p++) = u;
					}
				}

				node_pair_t* np = (node_pair_t*) (void*) eb;
				std::sort(np, np + eb_size, node_pair_comparator);

				//node_t last = LL_NIL_NODE;
				for (size_t i = 0; i < eb_size; i++) {
					node_t v = eb[i << 1];
					node_t u = eb[(i << 1) + 1];
					//if (v == last) continue;
					if (u >= v) continue;
					//last = v;

					G.out_iter_begin(iter2, v);
					size_t v_num = iter2.left;
					node_t* v_adj = (node_t*) iter2.ptr;

					G.out_iter_begin(iter, u);
					size_t u_num = iter.left;
					node_t* u_adj = (node_t*) iter.ptr;

					T_prv += count_triangles(u, v, u_adj, u_num,
							v_adj, v_num);
				}

				if ((u_start % 1024) == 0) {
					if ((u_start >> 10)%10 == 0) this->progress_update(u_start);
				}
			}

			ATOMIC_ADD<int64_t>(&T, T_prv);

			free(eb);
		}

		this->progress_clear();
		_num_triangles = T;
		return T; 
	}


	/**
	 * Print the results
	 * 
	 * @param f the output file
	 */
	virtual void print_results(FILE* f) {
		fprintf(f, "Number of triangles: %lld\n", (long long int) _num_triangles);
	}
};


/**
 * Triangle counting for undirected, single-level graphs loaded with the -OD
 * flags -- new version.
 */
template <>
class ll_b_triangle_counting_LOD<ll_writable_graph>
	: public ll_benchmark<ll_writable_graph> {

public:

	/**
	 * Create the benchmark
	 */
	ll_b_triangle_counting_LOD()
		: ll_benchmark<ll_writable_graph>("Triangle Counting") {
		LL_NOT_IMPLEMENTED;
	}


	/**
	 * Destroy the benchmark
	 */
	virtual ~ll_b_triangle_counting_LOD(void) {
	}


	/**
	 * Run the benchmark
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) {
		LL_NOT_IMPLEMENTED;
	}
};

#endif
