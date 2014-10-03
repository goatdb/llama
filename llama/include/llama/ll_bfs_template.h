/*
 * ll_bfs_template.h
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


#ifndef LL_BFS_TEMPLATE_H
#define LL_BFS_TEMPLATE_H
#include <omp.h>
#include <string.h>
#include <map>
#include <set>
#include <unordered_set>
#include <unordered_map>


template<class Graph, typename level_t, bool use_multithread, bool has_navigator,
	bool use_reverse_edge, bool save_child>
class ll_bfs_template
{

  public:
    ll_bfs_template(Graph& _G) :
            G(_G) {
        visited_bitmap = NULL; // bitmap
        visited_level = NULL;
        thread_local_next_level = NULL;
        down_edge_array = NULL;
        down_edge_set = NULL;
        down_edge_array_w = NULL;
        if (save_child) {
            down_edge_set = new std::unordered_set<edge_t>();
        }
    }

    virtual ~ll_bfs_template() {
        delete [] visited_bitmap;
        delete [] visited_level;
        delete [] thread_local_next_level;
        delete down_edge_set;

		if (down_edge_array != NULL) {
#ifndef FORCE_L0
			for (size_t i = 0; i < G.num_levels(); i++) delete[] down_edge_array[i];
#endif
			delete[] down_edge_array;
		}
    }

    void prepare(node_t root_node) {
		// TODO Is this correct? Do we need to poll a some sort of a runtime?
		prepare(root_node, omp_get_max_threads());
	}

    void prepare(node_t root_node, int max_num_thread) {
        int num_thread;
        if (use_multithread) {
            num_thread = max_num_thread;
        } else {
            num_thread = 1;
        }
		max_threads = num_thread;

        is_finished = false;
        curr_level = 0;
        root = root_node;
        state = ST_SMALL;
        assert(root != LL_NIL_NODE);
        if (save_child) {
            if (down_edge_set == NULL)
                down_edge_set = new std::unordered_set<edge_t>();
        }

        global_vector.clear();
        level_queue_begin.clear();
        level_count.clear();
        // create local queues
        if (thread_local_next_level == NULL) {
            thread_local_next_level = new std::vector<node_t>[num_thread];
            for (int i = 0; i < num_thread; i++)
                thread_local_next_level[i].reserve(THRESHOLD2);
        } else {
            for (int i = 0; i < num_thread; i++)
                thread_local_next_level[i].clear();
        }
    }

    void do_bfs_forward() {
        //---------------------------------
        // prepare root node
        //---------------------------------
        curr_level = 0;
        curr_count = 0;
        next_count = 0;

        small_visited[root] = curr_level;
        curr_count++;
        global_vector.push_back(root);
        global_curr_level_begin = 0;
        global_next_level_begin = curr_count;

        level_count.push_back(curr_count);
        level_queue_begin.push_back(global_curr_level_begin);

        bool is_done = false;
        while (!is_done) {
            switch (state) {
                case ST_SMALL: {
                    for (node_t i = 0; i < curr_count; i++) {
                        node_t t = global_vector[global_curr_level_begin + i];
                        iterate_neighbor_small(t);
                        visit_fw(t);            // visit after iteration. in that way, one can check  down-neighbors quite easily
                    }
                    break;
                }
                case ST_QUE: {
                    if (use_multithread)  // do it in parallel
                    {
                        int num_threads = std::min((node_t) max_threads, curr_count/128+1);
                        #pragma omp parallel num_threads(num_threads)
                        {
                            int tid = omp_get_thread_num();
                            #pragma omp for nowait 
                            for (node_t i = 0; i < curr_count; i++) {
                                node_t t = global_vector[global_curr_level_begin + i];
                                iterate_neighbor_que(t, tid);
                                visit_fw(t);
                            }
                            finish_thread_que(tid);
                        }
                    }
                    else { // do it in sequential
                            int tid = 0;
                            for (node_t i = 0; i < curr_count; i++) {
                                //node_t t = global_curr_level[i];
                                node_t t = global_vector[global_curr_level_begin + i];
                                iterate_neighbor_que(t, tid);
                                visit_fw(t);
                            }
                            finish_thread_que(tid);
                    }
                    break;
                }
                case ST_Q2R: {
                    if (use_multithread) {  // do it in parallel
                        int num_threads = std::min((node_t) max_threads, curr_count/128+1);
                        #pragma omp parallel num_threads(num_threads)
                        {
                            node_t local_cnt = 0;
                            #pragma omp for nowait 
                            for (node_t i = 0; i < curr_count; i++) {
                                node_t t = global_vector[global_curr_level_begin + i];
                                iterate_neighbor_rd(t, local_cnt);
                                visit_fw(t);
                            }
                            finish_thread_rd(local_cnt);
                        }
                    } else { // do it sequentially
                            node_t local_cnt = 0;
                            for (node_t i = 0; i < curr_count; i++) {
                                //node_t t = global_curr_level[i];
                                node_t t = global_vector[global_curr_level_begin + i];
                                iterate_neighbor_rd(t, local_cnt);
                                visit_fw(t);
                            }
                            finish_thread_rd(local_cnt);
                    }
                    break;
                }

                case ST_RD: {
                    if (use_multithread) { // do it in parallel
                        #pragma omp parallel
                        {
                            node_t local_cnt = 0;
                            #pragma omp for nowait schedule(dynamic,128)
                            for (node_t t = 0; t < G.max_nodes(); t++) {
                                if (visited_level[t] == curr_level) {
                                    iterate_neighbor_rd(t, local_cnt);
                                    visit_fw(t);
                                }
                            }
                            finish_thread_rd(local_cnt);
                        }
                    } else { // do it in sequential
                            node_t local_cnt = 0;
                            for (node_t t = 0; t < G.max_nodes(); t++) {
                                if (visited_level[t] == curr_level) {
                                    iterate_neighbor_rd(t, local_cnt);
                                    visit_fw(t);
                                }
                            }
                            finish_thread_rd(local_cnt);
                    }
                    break;
                }
                case ST_R2Q: {
                    if (use_multithread) { // do it in parallel
                        #pragma omp parallel
                        {
                            int tid = omp_get_thread_num();
                            #pragma omp for nowait schedule(dynamic,128)
                            for (node_t t = 0; t < G.max_nodes(); t++) {
                                if (visited_level[t] == curr_level) {
                                    iterate_neighbor_que(t, tid);
                                    visit_fw(t);
                                }
                            }
                            finish_thread_que(tid);
                        }
                    } else {
                            int tid = 0;
                            for (node_t t = 0; t < G.max_nodes(); t++) {
                                if (visited_level[t] == curr_level) {
                                    iterate_neighbor_que(t, tid);
                                    visit_fw(t);
                                }
                            }
                            finish_thread_que(tid);
                    }
                    break;
                }
            } // end of switch

            do_end_of_level_fw();
            is_done = get_next_state();

        } // end of while
    }

    void do_bfs_reverse() {
        // This function should be called only after do_bfs_foward has finished.
        // assumption: small-world graph
        level_t& level = curr_level;
        while (true) {
            node_t count = level_count[level];
            //node_t* queue_ptr = level_start_ptr[level];
            node_t* queue_ptr;
            node_t begin_idx = level_queue_begin[level];
            if (begin_idx == -1) { 
                queue_ptr = NULL;
            } else {
                queue_ptr = & (global_vector[begin_idx]);
            }
           
            if (queue_ptr == NULL) {
#pragma omp parallel if (use_multithread)
                {
#pragma omp for nowait schedule(dynamic,128)
                    for (node_t i = 0; i < G.max_nodes(); i++) {
                        if (visited_level[i] != curr_level) continue;
                        visit_rv(i);
                    }
                }
            } else {
				int num_threads = std::min((node_t) max_threads, curr_count/128+1);
#pragma omp parallel num_threads(num_threads) if (use_multithread)
                {
#pragma omp for nowait
                    for (node_t i = 0; i < count; i++) {
                        node_t u = queue_ptr[i];
                        visit_rv(u);
                    }
                }
            }

            do_end_of_level_rv();
            if (level == 0) break;
            level--;
        }
    }

    bool is_down_edge(edge_t idx) {
        if (state == ST_SMALL)
            return (down_edge_set->find(idx) != down_edge_set->end());
        else {
#ifdef FORCE_L0
            return down_edge_array[idx];
#else
			size_t level = LL_EDGE_LEVEL(idx);
			if (level == LL_WRITABLE_LEVEL) {
				return down_edge_array_w[LL_EDGE_GET_WRITABLE(idx)->we_numerical_id];
			}
			return down_edge_array[level][LL_EDGE_INDEX(idx)];
#endif
		}
    }

  protected:
    virtual void visit_fw(node_t t)=0;
    virtual void visit_rv(node_t t)=0;
    virtual bool check_navigator(node_t t, edge_t nx)=0;
    virtual void do_end_of_level_fw() {
    }
    virtual void do_end_of_level_rv() {
    }

    node_t get_root() {
        return root;
    }

    level_t get_level(node_t t) {
        // GCC expansion
        if (__builtin_expect((state == ST_SMALL), 0)) {
            if (small_visited.find(t) == small_visited.end())
                return __INVALID_LEVEL;
            else
                return small_visited[t];
        } else {
            return visited_level[t];
        }
    }

    level_t get_curr_level() {
        return curr_level;
    }


  private:
    bool get_next_state() {
        //const char* state_name[5] = {"SMALL","QUEUE","Q2R","RD","R2Q"};

        if (next_count == 0) return true;  // BFS is finished

        int next_state = state;
        switch (state) {
            case ST_SMALL:
                if (next_count >= THRESHOLD1) {
                    prepare_que();
                    next_state = ST_QUE;
                }
                break;
            case ST_QUE:
                if ((next_count >= THRESHOLD2) && (next_count >= curr_count*5)) {
                    prepare_read();
                    next_state = ST_Q2R;
                }
                break;
            case ST_Q2R:
                next_state = ST_RD;
                break;
            case ST_RD:
                if (next_count <= (2 * curr_count)) {
                    next_state = ST_R2Q;
                }
                break;
            case ST_R2Q:
                next_state = ST_QUE;
                break;
        }

        finish_level(state);
        state = next_state;

        return false;
    }

    void finish_level(int state) {
        if ((state == ST_RD) || (state == ST_Q2R)) {
            // output queue is not valid
        } else { // move output queue
            //node_t* temp = &(global_next_level[next_count]);
            //global_curr_level = global_next_level;
            //global_next_level = temp;
            global_curr_level_begin = global_next_level_begin;
            global_next_level_begin = global_next_level_begin + next_count;
        }

        curr_count = next_count;
        next_count = 0;
        curr_level++;

        // save 'new current' level status
        level_count.push_back(curr_count);
        if ((state == ST_RD) || (state == ST_Q2R)) {
            //level_start_ptr.push_back(NULL);
            level_queue_begin.push_back(-1);
        } else {
            //level_start_ptr.push_back(global_curr_level);
            level_queue_begin.push_back(global_curr_level_begin);
        }
    }

	void iter_begin(ll_edge_iterator& iter, node_t v) {
        if (use_reverse_edge) {
			G.in_iter_begin_fast(iter, v);
        } else {
			G.out_iter_begin(iter, v);
        }
    }

	edge_t iter_next(ll_edge_iterator& iter) {
        if (use_reverse_edge) {
            return G.in_iter_next_fast(iter);
        } else {
            return G.out_iter_next(iter);
        }
    }

    node_t get_node(ll_edge_iterator& iter) {
		return iter.last_node;
    }

    void iterate_neighbor_small(node_t t) {
		ll_edge_iterator iter; iter_begin(iter, t);
		for (edge_t nx = iter_next(iter); nx != LL_NIL_EDGE; nx = iter_next(iter)) {
            node_t u = get_node(iter);

            // check visited
            if (small_visited.find(u) == small_visited.end()) {
                if (has_navigator) {
                    if (check_navigator(u, nx) == false) continue;
                }

                if (save_child) {
                    save_down_edge_small(nx);
                }

                small_visited[u] = curr_level + 1;
                //global_next_level[next_count++] = u;
                global_vector.push_back(u); 
                next_count++;
            }
            else if (save_child) {
                if (has_navigator) {
                    if (check_navigator(u, nx) == false) continue;
                }

                if (small_visited[u] == (curr_level+1)){
                    save_down_edge_small(nx);
                }
            }
        }
    }

    // should be used only when save_child is enabled
    void save_down_edge_small(edge_t idx) {
        down_edge_set->insert(idx);
    }

    void save_down_edge_large(edge_t idx) {
#ifdef FORCE_L0
        down_edge_array[idx] = 1;
#else
		size_t level = LL_EDGE_LEVEL(idx);
		if (level == LL_WRITABLE_LEVEL) {
			down_edge_array_w[LL_EDGE_GET_WRITABLE(idx)->we_numerical_id] = 1;
		}
		down_edge_array[LL_EDGE_LEVEL(idx)][LL_EDGE_INDEX(idx)] = 1;
#endif
	}

    void prepare_que() {

        global_vector.reserve(G.max_nodes());

        // create bitmap and edges
        if (visited_bitmap == NULL) {
            visited_bitmap = new unsigned char[(G.max_nodes() + 7) / 8];
            visited_level = new level_t[G.max_nodes()];
        }
        if (save_child) {
            if (down_edge_array == NULL) {
#ifdef FORCE_L0
                down_edge_array = new unsigned char [G.max_edges(0)];
#else
                down_edge_array = new unsigned char* [G.num_levels()];
				for (size_t i = 0; i < G.num_levels(); i++) 
					down_edge_array[i] = new unsigned char [G.max_edges(i)];
				// Note: This makes sense only if the current graph is writable,
				// but fortunatelly it is never accessed unless we are on the
				// writable level
				down_edge_array_w = down_edge_array[G.num_levels() - 1];
#endif
			}
        }

        if (use_multithread) {
			#pragma omp parallel
            {
				#pragma omp for nowait
                for (node_t i = 0; i < (G.max_nodes() + 7) / 8; i++)
                    visited_bitmap[i] = 0;

				#pragma omp for nowait
                for (node_t i = 0; i < G.max_nodes(); i++)
                    visited_level[i] = __INVALID_LEVEL;

                if (save_child) {
#ifdef FORCE_L0
					#pragma omp for nowait
					for (edge_t i = 0; i < G.max_edges(0); i++) 
                        down_edge_array[i] = 0;
#else
					#pragma omp for nowait
					for (size_t i = 0; i < G.num_levels(); i++) 
                        memset(down_edge_array[i], 0, sizeof(unsigned char) * G.max_edges(i));
#endif
                }
            }
        } else {
            for (node_t i = 0; i < (G.max_nodes() + 7) / 8; i++)
                visited_bitmap[i] = 0;
            for (node_t i = 0; i < G.max_nodes(); i++)
                visited_level[i] = __INVALID_LEVEL;
            if (save_child) {
#ifdef FORCE_L0
				for (edge_t i = 0; i < G.max_edges(0); i++) 
					down_edge_array[i] = 0;
#else
				for (size_t i = 0; i < G.num_levels(); i++) 
					memset(down_edge_array[i], 0, sizeof(unsigned char) * G.max_edges(i));
#endif
            }
        }

        //typename std::unordered_map<node_t, level_t>::iterator II;
        typename std::map<node_t, level_t>::iterator II;
        for (II = small_visited.begin(); II != small_visited.end(); II++) {
            node_t u = II->first;
            level_t lev = II->second;
            _ll_set_bit(visited_bitmap, u);
            visited_level[u] = lev;
        }

        if (save_child) {
            typename std::unordered_set<edge_t>::iterator J;
            for (J = down_edge_set->begin(); J != down_edge_set->end(); J++) {
				edge_t idx = *J;
#ifdef FORCE_L0
				down_edge_array[idx] = 1;
#else
				size_t level = LL_EDGE_LEVEL(idx);
				if (level == LL_WRITABLE_LEVEL) {
					down_edge_array_w[LL_EDGE_GET_WRITABLE(idx)->we_numerical_id] = 1;
				}
				down_edge_array[level][LL_EDGE_INDEX(idx)] = 1;
#endif
            }
        }
    }

    void iterate_neighbor_que(node_t t, int tid) {
		ll_edge_iterator iter; iter_begin(iter, t);
		for (edge_t nx = iter_next(iter); nx != LL_NIL_EDGE; nx = iter_next(iter)) {
            node_t u = get_node(iter);
			assert(u >= 0 && u < G.max_nodes());

            // check visited bitmap
            // test & test& set
            if (_ll_get_bit(visited_bitmap, u) == 0) {
                if (has_navigator) {
                    if (check_navigator(u, nx) == false) continue;
                }

                bool re_check_result;
                if (use_multithread) {
                    re_check_result = _ll_set_bit_atomic(visited_bitmap, u);
                } else {
                    re_check_result = true;
                    _ll_set_bit(visited_bitmap, u);
                }

                if (save_child) {
                    save_down_edge_large(nx);
                }

                if (re_check_result) {
                    // add to local q
                    thread_local_next_level[tid].push_back(u);
                    visited_level[u] = (curr_level + 1);
                }
            }
            else if (save_child) {
                if (has_navigator) {
                    if (check_navigator(u, nx) == false) continue;
                }
                if (visited_level[u] == (curr_level +1)) {
                    save_down_edge_large(nx);
                }
            }
        }
    }

    void finish_thread_que(int tid) {
        node_t local_cnt = thread_local_next_level[tid].size();
        //copy curr_cnt to next_cnt
        if (local_cnt > 0) {
            node_t old_idx = __sync_fetch_and_add(&next_count, local_cnt);
            // copy to global vector
            memcpy(&(global_vector[global_next_level_begin + old_idx]), 
                   &(thread_local_next_level[tid][0]), 
                   local_cnt * sizeof(node_t));
        }
        thread_local_next_level[tid].clear();
    }

    void prepare_read() {
        // nothing to do
    }

    void iterate_neighbor_rd(node_t t, node_t& local_cnt) {
		ll_edge_iterator iter; iter_begin(iter, t);
		for (edge_t nx = iter_next(iter); nx != LL_NIL_EDGE; nx = iter_next(iter)) {
            node_t u = get_node(iter);

            // check visited bitmap
            // test & test& set
            if (_ll_get_bit(visited_bitmap, u) == 0) {
                if (has_navigator) {
                    if (check_navigator(u, nx) == false) continue;
                }

                bool re_check_result;
                if (use_multithread) {
                    re_check_result = _ll_set_bit_atomic(visited_bitmap, u);
                } else {
                    re_check_result = true;
                    _ll_set_bit(visited_bitmap, u);
                }

                if (save_child) {
                    save_down_edge_large(nx);
                }

                if (re_check_result) {
                    // add to local q
                    visited_level[u] = curr_level + 1;
                    local_cnt++;
                }
            }
            else if (save_child) {
                if (has_navigator) {
                    if (check_navigator(u, nx) == false) continue;
                }
                if (visited_level[u] == (curr_level +1)) {
                    save_down_edge_large(nx);
                }
            }
        }
    }

    void finish_thread_rd(node_t local_cnt) {
        __sync_fetch_and_add(&next_count, local_cnt);
    }


    //-----------------------------------------------------
    //-----------------------------------------------------
    static const int ST_SMALL = 0;
    static const int ST_QUE = 1;
    static const int ST_Q2R = 2;
    static const int ST_RD = 3;
    static const int ST_R2Q = 4;
    static const int THRESHOLD1 = 128;  // single threaded
    static const int THRESHOLD2 = 1024; // move to RD-based

    // not -1. 
    //(why? because curr_level-1 might be -1, when curr_level = 0)
    static const level_t __INVALID_LEVEL = -2;

    int state;

    unsigned char* visited_bitmap; // bitmap
    level_t* visited_level; // assumption: small_world graph
    bool is_finished;
    level_t curr_level;
    node_t root;
    Graph& G;
    node_t curr_count;
    node_t next_count;

    //std::unordered_map<node_t, level_t> small_visited;
    std::map<node_t, level_t> small_visited;
    std::unordered_set<edge_t>* down_edge_set;
	unsigned char* down_edge_array_w;
#ifdef FORCE_L0
    unsigned char* down_edge_array;
#else
    unsigned char** down_edge_array;
#endif

    //node_t* global_next_level;
    //node_t* global_curr_level;
    //node_t* global_queue;
    std::vector<node_t> global_vector; 
    node_t global_curr_level_begin;
    node_t global_next_level_begin;

    //std::vector<node_t*> level_start_ptr;
    std::vector<node_t> level_queue_begin;
    std::vector<node_t> level_count;

    std::vector<node_t>* thread_local_next_level;

	int max_threads;
};

#endif
