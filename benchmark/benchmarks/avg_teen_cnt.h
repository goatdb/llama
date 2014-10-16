/*
 * avg_teen_cnt.h
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


#ifndef LL_GENERATED_CPP_AVG_TEEN_CNT_H
#define LL_GENERATED_CPP_AVG_TEEN_CNT_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <float.h>
#include <limits.h>
#include <cmath>
#include <algorithm>
#include <omp.h>

#include "benchmarks/benchmark.h"


/**
 * The average teen counting benchmark
 */
template <class Graph>
class ll_b_avg_teen_cnt : public ll_benchmark<Graph> {

	int K;
	int32_t* G_teen_cnt;
	ll_mlcsr_node_property<int32_t>* G_age;


public:

	/**
	 * Create the benchmark
	 *
	 * @param k the age threshold
	 */
	ll_b_avg_teen_cnt(int k) : ll_benchmark<Graph>("Average Teen Count") {

		K = k;

		this->create_auto_array_for_nodes(G_teen_cnt);
		this->create_auto_property(G_age, "age");
	}


	/**
	 * Destroy the benchmark
	 */
	virtual ~ll_b_avg_teen_cnt(void) {
	}


	/**
	 * Run the benchmark
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) {

		Graph& G = *this->_graph;

		float avg = 0.0 ;
		double _avg4 = 0.0 ;
		int64_t _cnt3 = 0 ;
		int32_t __S2 = 0 ;

		__S2 = 0 ;
		_cnt3 = 0 ;
#pragma omp parallel
		{
			int32_t __S2_prv = 0 ;
			int64_t _cnt3_prv = 0 ;

			_cnt3_prv = 0 ;
			__S2_prv = 0 ;

#pragma omp for nowait schedule(dynamic,4096)
			for (node_t n = 0; n < G.max_nodes(); n ++) 
			{
				int32_t __S1 = 0 ;

				__S1 = 0 ;
				ll_edge_iterator iter;
				G.inm_iter_begin(iter, n);
				for (node_t t = G.inm_iter_next(iter);
						t != LL_NIL_NODE;
						t = G.inm_iter_next(iter)) {

					if ((((*G_age)[t] >= 10) && ((*G_age)[t] < 20)))
					{
						__S1 = __S1 + 1 ;
					}
				}
				G.set_node_prop(G_teen_cnt, n, __S1);
				if (((*G_age)[n] > K))
				{
					__S2_prv = __S2_prv + G_teen_cnt[n] ;
					_cnt3_prv = _cnt3_prv + 1 ;
				}
			}
			ATOMIC_ADD<int64_t>(&_cnt3, _cnt3_prv);
			ATOMIC_ADD<int32_t>(&__S2, __S2_prv);
		}
		_avg4 = (0 == _cnt3)?0.000000:(__S2 / ((double)_cnt3)) ;
		avg = (float)_avg4 ;
		return avg; 
	}
};

#endif
