/*
 * bfs.h
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


#ifndef LL_BFS_H
#define LL_BFS_H

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
class bfs_bfs : public ll_bfs_template
    <Graph, short, true, false, false, false>
{
public:
    bfs_bfs(Graph& _G, node_t& _root, int32_t& _count)
    : ll_bfs_template<Graph, short, true, false, false, false>(_G),
    G(_G), root(_root), count(_count){}

private:  // list of varaibles
    Graph& G;
    node_t& root;
    int32_t& count;

protected:
    virtual void visit_fw(node_t v) 
    {
        ATOMIC_ADD<int32_t>(&count, 1);
    }

    virtual void visit_rv(node_t v) {}
    virtual bool check_navigator(node_t v, edge_t v_idx) {return true;}


};


/**
 * Count vertices in the given BFS traversal
 */
template <class Graph>
class ll_b_bfs : public ll_benchmark<Graph> {

	node_t root;


public:

	/**
	 * Create the benchmark
	 *
	 * @param r the root
	 */
	ll_b_bfs(node_t r)
		: ll_benchmark<Graph>("BFS - Count") {

		root = r;
	}


	/**
	 * Destroy the benchmark
	 */
	virtual ~ll_b_bfs(void) {
	}


	/**
	 * Run the benchmark
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) {

		Graph& G = *this->_graph;
		int32_t count = 0;

		bfs_bfs<Graph> _BFS(G, root, count);
		_BFS.prepare(root);
		_BFS.do_bfs_forward();

		return count; 
	}
};

#endif
