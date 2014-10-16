/*
 * property_stats.h
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


#ifndef LL_GENERATED_CPP_PROPERTY_STATS_H
#define LL_GENERATED_CPP_PROPERTY_STATS_H

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
 * Tool: Edge property stats
 */
template <class Graph, class value_t>
class ll_t_edge_property_stats : public ll_benchmark<Graph> {

	value_t _min;
	value_t _max;
	value_t _sum;
	double _mean;
    size_t _count;

	ll_mlcsr_edge_property<value_t>* _p;


public:

	/**
	 * Create the tool
	 *
	 * @param graph the graph
	 * @param name the weight property name
	 */
	ll_t_edge_property_stats(const char* name)
		: ll_benchmark<Graph>("Edge Property Stats") {

		this->create_auto_property(_p, name);
	}


	/**
	 * Destroy the tool
	 */
	virtual ~ll_t_edge_property_stats(void) {
	}


	/**
	 * Run the benchmark
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) {

		assert(sizeof(value_t) >= 4);

		Graph& G = *this->_graph;
		ll_mlcsr_edge_property<value_t>& p = *_p;

        _min = 0;
        _max = 0;
        _sum = 0;
        _mean = 0;
        _count = 0;

        bool first = true;

        //#pragma omp parallel for schedule(dynamic,4096)
        for (node_t n = 0; n < G.max_nodes(); n++) {
            ll_edge_iterator iter;
            G.out_iter_begin(iter, n);
            for (edge_t e = G.out_iter_next(iter);
                    e != LL_NIL_EDGE;
                    e = G.out_iter_next(iter)) {

                value_t v = p[e];

                if (first) {
                    _min = v;
                    _max = v;
                    first = false;
                }
                else {
                    if (v < _min) _min = v;
                    if (v > _max) _max = v;
                }

                _sum += v;
                _count++;

                //if (v > 1) LL_D_PRINT("%lx: %ld --> %ld [w=%d]\n",
                        //e, n, iter.last_node, (int) v);
			}
        }

        _mean = _sum / (double) _count;

		return 0;
	}


	/**
	 * Finalize the benchmark
	 *
	 * @return the updated numerical result, if applicable
	 */
	virtual double finalize(void) {
        return _mean;
	}


	/**
	 * Print the results
	 * 
	 * @param f the output file
	 */
	virtual void print_results(FILE* f) {

        bool floating = ll_is_type_floating_point(_p->type());

        if (floating) {
            fprintf(f, "Min  : %lf\n", (double) _min);
            fprintf(f, "Max  : %lf\n", (double) _max);
            fprintf(f, "Sum  : %lf\n", (double) _sum);
        }
        else {
            fprintf(f, "Min  : %ld\n", (long) _min);
            fprintf(f, "Max  : %ld\n", (long) _max);
            fprintf(f, "Sum  : %ld\n", (long) _sum);
        }
        fprintf(f, "Count: %lu\n", _count);
        fprintf(f, "Mean : %lf\n", _mean);
	}
};

#endif

