/*
 * bc_random.h
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


#ifndef LL_GENERATED_CPP_BC_RANDOM_H
#define LL_GENERATED_CPP_BC_RANDOM_H

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


// BFS/DFS definitions for the procedure
template <class Graph>
class bc_random_bfs : public ll_bfs_template
    <Graph, short, true, false, false, true>
{
public:
    bc_random_bfs(Graph& _G, float*& _G_BC, node_t& _s, 
        float*& _G_sigma, float*& _G_delta)
    : ll_bfs_template<Graph, short, true, false, false, true>(_G),
    G(_G), G_BC(_G_BC), s(_s), G_sigma(_G_sigma), G_delta(_G_delta){}

private:  // list of varaibles
    Graph& G;
    float*& G_BC;
    node_t& s;
    float*& G_sigma;
    float*& G_delta;

protected:
    virtual void visit_fw(node_t v) 
    {
        {
			ll_edge_iterator iter;
			G.out_iter_begin(iter, v);
			for (edge_t w_idx = G.out_iter_next(iter);
					w_idx != LL_NIL_EDGE;
					w_idx = G.out_iter_next(iter)) {
                if (!this->is_down_edge(w_idx)) continue;
				node_t w = LL_ITER_OUT_NEXT_NODE(G, iter, w_idx);
                float sigma_w_prv = 0.0 ;

                sigma_w_prv = ((float)(0.000000)) ;
                sigma_w_prv = sigma_w_prv + G_sigma[v] ;
				ATOMIC_ADD(&G_sigma[w], sigma_w_prv);
            }
        }
    }

    virtual void visit_rv(node_t v) 
    {
        if (v != s)
        {
            float __S3 = 0.0 ;

            __S3 = ((float)(0.000000)) ;
			ll_edge_iterator iter;
			G.out_iter_begin(iter, v);
			for (edge_t w_idx = G.out_iter_next(iter);
					w_idx != LL_NIL_EDGE;
					w_idx = G.out_iter_next(iter)) {
                if (!this->is_down_edge(w_idx)) continue;
				node_t w = LL_ITER_OUT_NEXT_NODE(G, iter, w_idx);
                __S3 = __S3 + G_sigma[v] / G_sigma[w] * (1 + G_delta[w]) ;
            }
            G.set_node_prop(G_delta, v, __S3);
            G.set_node_prop(G_BC, v, G_BC[v] + G_delta[v]);
        }
    }

    virtual bool check_navigator(node_t v, edge_t v_idx) {return true;}


};



/**
 * Betweenness Centrality - Randomized Algorithm
 */
template <class Graph>
class ll_b_bc_random : public ll_benchmark<Graph> {

	int K;
	float* G_BC;


public:

	/**
	 * Create the benchmark
	 *
	 * @param k the number of seeds
	 */
	ll_b_bc_random(int k)
		: ll_benchmark<Graph>("Betweenness Centrality - Randomized") {

		K = k;

		this->create_auto_array_for_nodes(G_BC);
	}


	/**
	 * Destroy the benchmark
	 */
	virtual ~ll_b_bc_random(void) {
	}


	/**
	 * Run the benchmark
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) {

		Graph& G = *this->_graph;
		int32_t k = 0 ;
		ll_memory_helper m;

		float* G_sigma = m.allocate<float>(G.max_nodes());
		float* G_delta = m.allocate<float>(G.max_nodes());

		k = 0 ;

#pragma omp parallel for
		for (node_t t0 = 0; t0 < G.max_nodes(); t0 ++) 
			G.set_node_prop(G_BC, t0, (float)0);

		while (k < K)
		{
			node_t s;

			s = G.pick_random_node() ;

#pragma omp parallel for
			for (node_t t1 = 0; t1 < G.max_nodes(); t1 ++) 
				G.set_node_prop(G_sigma, t1, (float)0);

			G.set_node_prop(G_sigma, s, (float)1);

			bc_random_bfs<Graph> _BFS(G, G_BC, s, G_sigma, G_delta);
			_BFS.prepare(s);
			_BFS.do_bfs_forward();
			_BFS.do_bfs_reverse();
			k = k + 1 ;
		}

		return 0;
	}


	/**
	 * Finalize the benchmark
	 *
	 * @return the updated numerical result, if applicable
	 */
	virtual double finalize(void) {
		float max  = 0;

		for (node_t n = 0; n < this->_graph->max_nodes(); n++) {
			if (G_BC[n] > max) max = G_BC[n];
		}

		return max;
	}


	/**
	 * Print the results
	 * 
	 * @param f the output file
	 */
	virtual void print_results(FILE* f) {
		print_results_part(f, this->_graph, G_BC);
	}
};

#endif
