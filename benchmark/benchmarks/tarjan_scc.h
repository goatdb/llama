/*
 * tarjan_scc.h
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


#ifndef LL_GENERATED_CPP_TARJAN_SCC_H
#define LL_GENERATED_CPP_TARJAN_SCC_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <float.h>
#include <limits.h>
#include <cmath>
#include <algorithm>
#include <omp.h>

#include "llama/ll_dfs_template.h"
#include "llama/ll_writable_graph.h"
#include "llama/ll_seq.h"
#include "benchmarks/benchmark.h"


// BFS/DFS definitions for the procedure
template <class Graph>
class Tarjan_dfs : public ll_dfs_template
    <Graph, true, true, true, false>
{
public:
    Tarjan_dfs(Graph& _G, node_t*& _G_SCC, bool*& _G_InStack, 
        node_t*& _G_LowLink, ll_node_seq_vec& _Stack, node_t& _n)
    : ll_dfs_template<Graph, true, true, true, false>(_G),
      G(_G), G_SCC(_G_SCC), G_InStack(_G_InStack), G_LowLink(_G_LowLink),
	  Stack(_Stack), n(_n){}

private:  // list of varaibles
    Graph& G;
    node_t*& G_SCC;
    bool*& G_InStack;
    node_t*& G_LowLink;
    ll_node_seq_vec& Stack;
    node_t& n;

protected:
    virtual void visit_pre(node_t t) 
    {
        G.set_node_prop(G_InStack, t, true);
        Stack.push_back(t);
        G.set_node_prop(G_LowLink, t, t);
    }

    virtual void visit_post(node_t t) 
    {
		ll_edge_iterator iter;
		G.out_iter_begin(iter, t);
		FOREACH_OUTEDGE_ITER(k_idx, G, iter) {
            node_t k = iter.last_node;
            if (G_LowLink[k] < G_LowLink[t])
                G.set_node_prop(G_LowLink, t, G_LowLink[k]);
        }
        if (G_LowLink[t] == t)
        {
            node_t w;

            w = Stack.pop_back() ;
            while (w != t)
            {
                G.set_node_prop(G_InStack, w, false);
                G.set_node_prop(G_SCC, w, t);
                w = Stack.pop_back() ;
            }
            G.set_node_prop(G_InStack, w, false);
            G.set_node_prop(G_SCC, w, t);
        }
    }

    virtual bool check_navigator(node_t t, edge_t t_idx) 
    {
        return ( !G_InStack[t]);
    }


};



/**
 * Tarjan's SCC
 */
template <class Graph>
class ll_b_tarjan_scc : public ll_benchmark<Graph> {

	node_t* G_SCC;


public:

	/**
	 * Create the benchmark
	 */
	ll_b_tarjan_scc() : ll_benchmark<Graph>("Tarjan's SCC") {
		this->create_auto_array_for_nodes(G_SCC);
	}


	/**
	 * Destroy the benchmark
	 */
	virtual ~ll_b_tarjan_scc(void) {
	}


	/**
	 * Run the benchmark
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) {

		//Initializations
		Graph& G = *this->_graph;
		ll_memory_helper m;

		ll_node_seq_vec Stack(omp_get_max_threads());
		bool* G_InStack = m.allocate<bool>(G.max_nodes());
		node_t* G_LowLink = m.allocate<node_t>(G.max_nodes());


		#pragma omp parallel for
		for (node_t t0 = 0; t0 < G.max_nodes(); t0 ++) 
		{
			G.set_node_prop(G_SCC, t0, LL_NIL_NODE);
			G.set_node_prop(G_InStack, t0, false);
		}
		for (node_t n = 0; n < G.max_nodes(); n ++) 
		{
			if (G_SCC[n] == LL_NIL_NODE)
			{

				Tarjan_dfs<Graph> _DFS(G, G_SCC, G_InStack, G_LowLink,
						Stack, n);
				_DFS.prepare(n);
				_DFS.do_dfs();
			}
		}

		return 0;
	}


	/**
	 * Finalize the benchmark
	 *
	 * @return the updated numerical result, if applicable
	 */
	virtual double finalize(void) {
		node_t max  = 0;

		for (node_t n = 0; n < this->_graph->max_nodes(); n++) {
			if (G_SCC[n] > max) max = G_SCC[n];
		}

		return max;
	}


	/**
	 * Print the results
	 * 
	 * @param f the output file
	 */
	virtual void print_results(FILE* f) {
		print_results_part(f, this->_graph, G_SCC);
	}
};

#endif
