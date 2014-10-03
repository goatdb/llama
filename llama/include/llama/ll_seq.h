/*
 * ll_seq.h
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


#ifndef _LL_SEQ_H
#define _LL_SEQ_H

// From: Green-Marl/apps/output_cpp/gm_graph/inc/gm_seq.h

#include <stdio.h>
#include <list>

template<typename IterType, typename T>
class Seq_Iterator
{
private:
    IterType iter;
    IterType end;

public:
    Seq_Iterator(IterType iterator_begin, IterType iterator_end) :
            iter(iterator_begin), end(iterator_end) {
    }

    inline bool has_next() {
        return iter != end;
    }

    inline T get_next() {
        T value = *iter;
        iter++;
        return value;
    }
};

template<typename T>
class ll_seq
{
public:
    ll_seq() {
		// TODO Is this right?
        init(omp_get_max_threads());
    }
    ll_seq(int _max_thread) {
        init(_max_thread);
    }

    virtual ~ll_seq() {
        delete[] local_Q_front;
        delete[] local_Q_back;
    }

    //------------------------------------------------------------
    // API
    //   push_back/front, pop_back/front, clear, get_size
    //   push has separate parallel interface
    //------------------------------------------------------------
    void push_back(T e) {
        Q.push_back(e);
    }

    void push_front(T e) {
        Q.push_front(e);
    }

    T pop_back() {
        T e = Q.back();
        Q.pop_back();
        return e;
    }

    T pop_front() {
        T e = Q.front();
        Q.pop_front();
        return e;
    }

    void clear() {
        Q.clear();
    }

    int get_size() {
        return Q.size();
    }

    // for parallel execution
    void push_back_par(T e, int tid) {
        local_Q_back[tid].push_back(e);
    }
    void push_front_par(T e, int tid) {
        local_Q_front[tid].push_front(e);
    }

    // parallel pop is prohibited

    //-------------------------------------------
    // called when parallel addition is finished
    //-------------------------------------------
    void merge() {
        for (int i = 0; i < max_thread; i++) {
            if (local_Q_front[i].size() > 0) Q.splice(Q.begin(), local_Q_front[i]);

            if (local_Q_back[i].size() > 0) Q.splice(Q.end(), local_Q_back[i]);
        }
    }

    typename std::list<T>& get_list() {
        return Q;
    }

    typedef Seq_Iterator<typename std::list<T>::iterator, T> seq_iter;
    typedef Seq_Iterator<typename std::list<T>::reverse_iterator, T> rev_iter;
    typedef seq_iter par_iter; // type-alias

    seq_iter prepare_seq_iteration() {
        seq_iter I(Q.begin(), Q.end());
        return I; // copy return
    }
    rev_iter prepare_rev_iteration() {
        rev_iter I(Q.rbegin(), Q.rend());
        return I; // copy return
    }

    // [xxx] to be implemented
    par_iter prepare_par_iteration(int thread_id, int max_threads) {
        assert(false);
        return NULL;
    }

private:

    typename std::list<T> Q;
    typename std::list<T>* local_Q_front;
    typename std::list<T>* local_Q_back;

    int max_thread;
    static const int THRESHOLD = 1024;

    void init(int _max_thread) {
        max_thread = _max_thread;
        if (_max_thread > THRESHOLD) {
            printf("error, too many # threads:%d\n", _max_thread);
            abort();
            max_thread = THRESHOLD;
        }
        local_Q_front = new std::list<T>[max_thread];
        local_Q_back = new std::list<T>[max_thread];
    }
};

typedef ll_seq<node_t> ll_node_seq;
typedef ll_seq<edge_t> ll_edge_seq;

template<typename T>
class ll_seq_vec
{
private:

    typename std::vector<T> data;
    typename std::vector<T>* local_back;
    typename std::vector<T>* local_front;

    int max_thread;
    static const int THRESHOLD = 1024;

    void init(int _max_thread) {
        if (_max_thread > THRESHOLD) {
            printf("error, too many # threads:%d\n", _max_thread);
            abort();
            max_thread = THRESHOLD;
        }
        max_thread = _max_thread;
        local_back = new std::vector<T>[max_thread];
        local_front = new std::vector<T>[max_thread];
    }

public:
    ll_seq_vec() {
		// TODO Is this right?
        init(omp_get_max_threads());
    }
    ll_seq_vec(int _max_thread) {
        init(_max_thread);
    }

    virtual ~ll_seq_vec() {
        delete[] local_front;
        delete[] local_back;
    }

    //------------------------------------------------------------
    // API
    //   push_back/front, pop_back/front, clear, get_size
    //   push has separate parallel interface
    //------------------------------------------------------------
    void push_back(T e) {
        data.push_back(e);
    }

    void push_front(T e) {
        assert(false); //not meant to be used this way
    }

    T pop_back() {
        T e = data.back();
        data.pop_back();
        return e;
    }

    T pop_front() {
        assert(false); //not meant to be used this way
        return NULL;
    }

    void clear() {
        data.clear();
    }

    int get_size() {
        return data.size();
    }

    // for parallel execution
    void push_back_par(T e, int tid) {
        local_back[tid].push_back(e);
    }

    void push_front_par(T e, int tid) {
        local_front[tid].push_front(e);
    }

    // parallel pop is prohibited

    //-------------------------------------------
    // called when parallel addition is finished
    //-------------------------------------------
    void merge() {
        for (int i = 0; i < max_thread; i++) {
            if (local_front[i].size() > 0) {
                data.splice(data.begin(), local_front[i]);
            }
            if (local_back[i].size() > 0) {
                data.splice(data.end(), local_back[i]);
            }
        }
    }

    typedef Seq_Iterator<typename std::vector<T>::iterator, T> seq_iter;
    typedef Seq_Iterator<typename std::vector<T>::reverse_iterator, T> rev_iter;
    typedef seq_iter par_iter; // type-alias

    seq_iter prepare_seq_iteration() {
        seq_iter I(data.begin(), data.end());
        return I; // copy return
    }
    rev_iter prepare_rev_iteration() {
        rev_iter I(data.rbegin(), data.rend());
        return I; // copy return
    }

    // [xxx] to be implemented
    par_iter prepare_par_iteration(int thread_id, int max_threads) {
        assert(false);
        return NULL;
    }
};

typedef ll_seq_vec<node_t> ll_node_seq_vec;
typedef ll_seq_vec<edge_t> ll_edge_seq_vec;

#endif
