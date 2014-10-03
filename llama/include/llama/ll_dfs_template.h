/*
 * ll_dfs_template.h
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

/*
 * This file was adapted from Green-Marl, which includes the following notice:
 *
 * Copyright (c) 2011-2012 Stanford University, unless otherwise specified.
 * All rights reserved.
 *
 * This software was developed by the Pervasive Parallelism Laboratory of
 * Stanford University, California, USA.
 *
 * Permission to use, copy, modify, and distribute this software in source or
 * binary form for any purpose with or without fee is hereby granted, provided
 * that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of Stanford University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */


#ifndef LL_DFS_TEMPLATE_H
#define LL_DFS_TEMPLATE_H

#include <omp.h>
#include <string.h>
#include <set>
#include <unordered_set>
#include <vector>


template<class Graph, bool has_pre_visit, bool has_post_visit, bool has_navigator, bool use_reverse_edge>
class ll_dfs_template
{
  protected:
    virtual void visit_pre(node_t t)=0;
    virtual void visit_post(node_t t)=0;
    virtual bool check_navigator(node_t t, edge_t idx)=0;

  public:
    ll_dfs_template(Graph& _G) :
            G(_G) {
        visited_bitmap = NULL; // bitmap
    }

    virtual ~ll_dfs_template() {
        delete visited_bitmap;
    }

    void prepare(node_t root_node) {
        root = root_node;
        cnt = 0;
        visited_small.clear();

        is_small = true;
		iter.node = INVALID_NODE;
        THRESHOLD_LARGE = std::max((int)(G.max_nodes()*0.1), 4096); 
    }

    void do_dfs() {
        enter_node(root);
        main_loop();
    }

  private:
    void prepare_large() {
        delete[] visited_bitmap;

        visited_bitmap = new unsigned char[(G.max_nodes() + 7) / 8];

        #pragma omp parallel for schedule(dynamic,16384)
        for (int i = 0; i < (G.max_nodes() + 7) / 8; i++)
            visited_bitmap[i] = 0;

        std::unordered_set<node_t>::iterator I;
        for (I = visited_small.begin(); I != visited_small.end(); I++) {
            node_t u = *I;
            _ll_set_bit(visited_bitmap, u);
        }
        is_small = false;
        stack.reserve(G.max_nodes());
    }

    void enter_node(node_t n) {
        // push current node
        stack.push_back(iter);

		if (use_reverse_edge)
			G.in_iter_begin_fast(iter, n);
		else
			G.out_iter_begin(iter, n);

        // mark visited
        add_visited(n);
        cnt++;
        if (cnt == THRESHOLD_LARGE) {
            prepare_large();
        }

        if (has_pre_visit) visit_pre(n);
    }

    void exit_node(node_t n) {
        if (has_post_visit) visit_post(n);
        iter = stack.back();
        stack.pop_back();
    }

    void main_loop() {
        //----------------------------------
        // Repeat until stack is empty
        //----------------------------------
        while (iter.node != INVALID_NODE) {
            //----------------------------------
            // Every neighbor has been visited
            //----------------------------------
            if (iter.edge == LL_NIL_EDGE) {
                exit_node(iter.node);
                continue;
            }

            else {
                //----------------------------------
                // check every non-visited neighbor
                //----------------------------------
                node_t z;
				edge_t e;
                if (use_reverse_edge) {
                    e = G.in_iter_next_fast(iter);
                } else {
                    e = G.out_iter_next(iter);
                }
				assert(e != LL_NIL_EDGE);
				z = iter.last_node;

                if (has_visited(z)) {
                    continue;
                }
                if (has_navigator) {
                    if (check_navigator(z, e) == false) {
                        continue;
                    }
                }
                enter_node(z);
                continue;
            }
        }
    }

    void add_visited(node_t n) {
        if (is_small)
            visited_small.insert(n);
        else
            _ll_set_bit(visited_bitmap, n);
    }

    bool has_visited(node_t n) {
        if (is_small) {
            return (visited_small.find(n) != visited_small.end());
        } else {
            return _ll_get_bit(visited_bitmap, n);
        }
    }

  protected:
    node_t root;
    Graph& G;

    // stack implementation
    node_t stack_ptr;
    std::vector<ll_edge_iterator> stack;
	ll_edge_iterator iter;

    // visited set implementation
    node_t cnt;
    unsigned char* visited_bitmap;
    std::unordered_set<node_t> visited_small;
    bool is_small;
    int THRESHOLD_LARGE;
    static const node_t INVALID_NODE = -1;

};

#endif
