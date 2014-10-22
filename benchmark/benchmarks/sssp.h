/*
 * sssp.h
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


#ifndef LL_GENERATED_CPP_SSSP_H
#define LL_GENERATED_CPP_SSSP_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <float.h>
#include <limits.h>
#include <cmath>
#include <algorithm>
#include <omp.h>

#include "llama/ll_bfs_template.h"
#include "llama/ll_writable_graph.h"
#include "benchmarks/benchmark.h"

#define LL_SSSP_RETURNS_MAX


/**
 * Weighted SSSP
 */
template <class Graph, class WeightType>
class ll_b_sssp_weighted : public ll_benchmark<Graph> {

	node_t root;
	WeightType* G_dist;
	ll_mlcsr_edge_property<WeightType>* G_weight;


public:

	/**
	 * Create the benchmark
	 *
	 * @param root the root
	 * @param weightName the weight property name
	 */
	ll_b_sssp_weighted(node_t root, const char* weightName)
		: ll_benchmark<Graph>("SSSP - Weighted") {

		this->root = root;

		this->create_auto_array_for_nodes(G_dist);
		this->create_auto_property(G_weight, weightName);
	}


	/**
	 * Destroy the benchmark
	 */
	virtual ~ll_b_sssp_weighted(void) {
	}


	/**
	 * Run the benchmark
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) {

		assert(sizeof(WeightType) >= 4);

		Graph& G = *this->_graph;
		ll_mlcsr_edge_property<WeightType>& G_len = *G_weight;
		ll_spinlock_table lt;
		ll_memory_helper m;

		bool fin = false ;
		bool* G_updated = m.allocate<bool>(G.max_nodes());
		bool* G_updated_nxt = m.allocate<bool>(G.max_nodes());
		WeightType* G_dist_nxt
			= (WeightType*) malloc(sizeof(WeightType) * G.max_nodes());

		fin = false ;

#pragma omp parallel for
		for (node_t t0 = 0; t0 < G.max_nodes(); t0 ++) 
		{
			// Assume that INT_MAX-1 is high enough, even if WeightType != int,
			// and also for now assume that WeightType is at least 4 bytes long
			G.set_node_prop(G_dist, t0, (WeightType) ((t0 == root)?0:INT_MAX-1));
			G.set_node_prop(G_updated, t0, (t0 == root)?true:false);
			G.set_node_prop(G_dist_nxt, t0, G_dist[t0]);
			G.set_node_prop(G_updated_nxt, t0, G_updated[t0]);
		}
		while ( !fin)
		{
			bool __E8 = false ;

			fin = true ;
			__E8 = false ;

#pragma omp parallel for schedule(dynamic,4096)
			for (node_t n = 0; n < G.max_nodes(); n ++) 
			{
				if (G_updated[n])
				{
					ll_edge_iterator iter;
					G.out_iter_begin(iter, n);
					for (edge_t s_idx = G.out_iter_next(iter);
							s_idx != LL_NIL_EDGE;
							s_idx = G.out_iter_next(iter)) {
						node_t s = LL_ITER_OUT_NEXT_NODE(G, iter, s_idx);
						edge_t e;

						e = s_idx ;
						{ // argmin(argmax) - test and test-and-set
							WeightType G_dist_nxt_new = G_dist[n] + G_len[e];
							if (G_dist_nxt[s]>G_dist_nxt_new) {
								bool G_updated_nxt_arg = true;
								lt.acquire_for(s);
								if (G_dist_nxt[s]>G_dist_nxt_new) {
									G.set_node_prop(G_dist_nxt, s,
											G_dist_nxt_new);
									G.set_node_prop(G_updated_nxt, s,
											G_updated_nxt_arg);
								}
								lt.release_for(s);
							}
						}
					}
				}
			}
#pragma omp parallel
			{
				bool __E8_prv = false ;

				__E8_prv = false ;

#pragma omp for nowait
				for (node_t t4 = 0; t4 < G.max_nodes(); t4 ++) 
				{
					G.set_node_prop(G_dist, t4, G_dist_nxt[t4]);
					G.set_node_prop(G_updated, t4, G_updated_nxt[t4]);
					G.set_node_prop(G_updated_nxt, t4, false);
					__E8_prv = __E8_prv || G_updated[t4] ;
				}
				ATOMIC_OR(&__E8, __E8_prv);
			}
			fin =  !__E8 ;
		}

		free(G_dist_nxt);
		return 0;
	}


	/**
	 * Finalize the benchmark
	 *
	 * @return the updated numerical result, if applicable
	 */
	virtual double finalize(void) {
		size_t count = 0;
		int32_t max = 0;
		for (node_t n = 0; n < this->_graph->max_nodes(); n++) {
			if (G_dist[n] < INT_MAX-1) {
				count++;
				if (G_dist[n] > max) max = G_dist[n];
			}
		}
#ifdef LL_SSSP_RETURNS_MAX
		return max;
#else
		return count;
#endif
	}


	/**
	 * Print the results
	 * 
	 * @param f the output file
	 */
	virtual void print_results(FILE* f) {
		print_results_part(f, this->_graph, G_dist);
	}
};


// BFS/DFS definitions for the procedure
template <class Graph>
class u_sssp_bfs : public ll_bfs_template
    <Graph, short, true, false, false, false>
{
public:
    u_sssp_bfs(Graph& _G, node_t& _root, int32_t* _dist)
    : ll_bfs_template<Graph, short, true, false, false, false>(_G),
    G(_G), root(_root), dist(_dist){}

private:  // list of varaibles
    Graph& G;
    node_t& root;
	int32_t* dist;

protected:
    virtual void visit_fw(node_t v) 
    {
		dist[v] = this->get_curr_level();
    }

    virtual void visit_rv(node_t v) {}
    virtual bool check_navigator(node_t v, edge_t v_idx) {return true;}


};



/**
 * Unweighted SSSP
 */
template <class Graph>
class ll_b_sssp_unweighted_bfs : public ll_benchmark<Graph> {

	node_t root;
	int32_t* G_dist;


public:

	/**
	 * Create the benchmark
	 *
	 * @param root the root
	 */
	ll_b_sssp_unweighted_bfs(node_t root)
		: ll_benchmark<Graph>("SSSP - Unweighted, BFS") {

		this->root = root;
		this->create_auto_array_for_nodes(G_dist);
	}


	/**
	 * Destroy the benchmark
	 */
	virtual ~ll_b_sssp_unweighted_bfs(void) {
	}


	/**
	 * Run the benchmark
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) {

		Graph& G = *this->_graph;

#pragma omp parallel for
		for (node_t t0 = 0; t0 < G.max_nodes(); t0++) {
			G.set_node_prop(G_dist, t0, INT_MAX-1);
		}
		G.set_node_prop(G_dist, root, 0);

		u_sssp_bfs<Graph> _BFS(G, root, G_dist);
		_BFS.prepare(root);
		_BFS.do_bfs_forward();

		return 0; 
	}


	/**
	 * Finalize the benchmark
	 *
	 * @return the updated numerical result, if applicable
	 */
	virtual double finalize(void) {
		size_t count = 0;
		int32_t max = 0;
		for (node_t n = 0; n < this->_graph->max_nodes(); n++) {
			if (G_dist[n] < INT_MAX-1) {
				count++;
				if (G_dist[n] > max) max = G_dist[n];
			}
		}
#ifdef LL_SSSP_RETURNS_MAX
		return max;
#else
		return count;
#endif
	}


	/**
	 * Print the results
	 * 
	 * @param f the output file
	 */
	virtual void print_results(FILE* f) {
		print_results_part(f, this->_graph, G_dist);
	}
};



/**
 * Unweighted SSSP
 */
template <class Graph>
class ll_b_sssp_unweighted_iter : public ll_benchmark<Graph> {

	node_t root;
	int32_t* G_dist;


public:

	/**
	 * Create the benchmark
	 *
	 * @param root the root
	 */
	ll_b_sssp_unweighted_iter(node_t root)
		: ll_benchmark<Graph>("SSSP - Unweighted, iterative") {

		this->root = root;
		this->create_auto_array_for_nodes(G_dist);
	}


	/**
	 * Destroy the benchmark
	 */
	virtual ~ll_b_sssp_unweighted_iter(void) {
	}


	/**
	 * Run the benchmark
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) {

		Graph& G = *this->_graph;
		ll_spinlock_table lt;
		ll_memory_helper m;

		bool fin = false ;
		bool* G_updated = m.allocate<bool>(G.max_nodes());
		bool* G_updated_nxt = m.allocate<bool>(G.max_nodes());
		int32_t* G_dist_nxt
			= (int32_t*) malloc(sizeof(int32_t) * G.max_nodes());

		fin = false ;

#pragma omp parallel for
		for (node_t t0 = 0; t0 < G.max_nodes(); t0 ++) 
		{
			G.set_node_prop(G_dist, t0, (t0 == root)?0:INT_MAX-1);
			G.set_node_prop(G_updated, t0, (t0 == root)?true:false);
			G.set_node_prop(G_dist_nxt, t0, G_dist[t0]);
			G.set_node_prop(G_updated_nxt, t0, G_updated[t0]);
		}
		while ( !fin)
		{
			bool __E8 = false ;

			fin = true ;
			__E8 = false ;

#pragma omp parallel for schedule(dynamic,4096)
			for (node_t n = 0; n < G.max_nodes(); n ++) 
			{
				if (G_updated[n])
				{
					ll_edge_iterator iter;
					G.out_iter_begin(iter, n);
					for (edge_t s_idx = G.out_iter_next(iter);
							s_idx != LL_NIL_EDGE;
							s_idx = G.out_iter_next(iter)) {
						node_t s = LL_ITER_OUT_NEXT_NODE(G, iter, s_idx);

						{ // argmin(argmax) - test and test-and-set
							int32_t G_dist_nxt_new = G_dist[n] + 1;
							if (G_dist_nxt[s]>G_dist_nxt_new) {
								bool G_updated_nxt_arg = true;
								lt.acquire_for(s);
								if (G_dist_nxt[s]>G_dist_nxt_new) {
									G.set_node_prop(G_dist_nxt, s, G_dist_nxt_new);
									G.set_node_prop(G_updated_nxt, s, G_updated_nxt_arg);
								}
								lt.release_for(s);
							}
						}
					}
				}
			}
#pragma omp parallel
			{
				bool __E8_prv = false ;

#pragma omp for nowait
				for (node_t t4 = 0; t4 < G.max_nodes(); t4 ++) 
				{
					G.set_node_prop(G_dist, t4, G_dist_nxt[t4]);
					G.set_node_prop(G_updated, t4, G_updated_nxt[t4]);
					G.set_node_prop(G_updated_nxt, t4, false);
					__E8_prv = __E8_prv || G_updated[t4] ;
				}
				ATOMIC_OR(&__E8, __E8_prv);
			}
			fin =  !__E8 ;
		}

		free(G_dist_nxt);
		return 0;
	}


	/**
	 * Finalize the benchmark
	 *
	 * @return the updated numerical result, if applicable
	 */
	virtual double finalize(void) {
		size_t count = 0;
		int32_t max = 0;
		for (node_t n = 0; n < this->_graph->max_nodes(); n++) {
			if (G_dist[n] < INT_MAX-1) {
				count++;
				if (G_dist[n] > max) max = G_dist[n];
			}
		}
#ifdef LL_SSSP_RETURNS_MAX
		return max;
#else
		return count;
#endif
	}


	/**
	 * Print the results
	 * 
	 * @param f the output file
	 */
	virtual void print_results(FILE* f) {
		print_results_part(f, this->_graph, G_dist);
	}
};

#endif

