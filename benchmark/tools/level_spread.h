/*
 * level_spread.h
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


#ifndef LL_LEVEL_SPREAD_H
#define LL_LEVEL_SPREAD_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <float.h>
#include <limits.h>
#include <cmath>
#include <algorithm>
#include <omp.h>

#include "benchmarks/benchmark.h"


template <class EXP_GRAPH>
void level_spread(EXP_GRAPH& G, std::vector<int>& r, std::vector<int>& ds)
{
	r.clear();
	int nl = G.num_levels();
	for (int i = 0; i < nl+1; i++) r.push_back(0);
	for (int i = 0; i < nl+1; i++) ds.push_back(0);
	bool* b = (bool*) alloca(sizeof(bool) * (nl+1));

	for (node_t n = 0; n < G.max_nodes(); n++) {
		bzero(b, sizeof(bool) * (nl+1));

		ll_edge_iterator iter;
		G.out_iter_begin(iter, n);
		for (edge_t e = G.out_iter_next(iter);
				e != LL_NIL_EDGE;
				e = G.out_iter_next(iter)) {

			edge_t l = LL_EDGE_LEVEL(e);
			if (l == LL_WRITABLE_LEVEL) l = nl;
			b[l] = true;
		}

		int x = 0;
		for (int i = 0; i < nl+1; i++) {
			if (b[i]) x++;
		}

		r[x]++;
		ds[x] += G.out_degree(n);
	}
}


/**
 * Tool: Level spread
 */
template <class Graph>
class ll_t_level_spread : public ll_benchmark<Graph> {

    std::vector<int> R_level_spread;
    std::vector<int> R_degree_sum;


public:

	/**
	 * Create the tool
	 */
	ll_t_level_spread() : ll_benchmark<Graph>("[Tool] Level Spread") {
	}


	/**
	 * Destroy the tool
	 */
	virtual ~ll_t_level_spread(void) {
	}


	/**
	 * Run the tool
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) {

		level_spread(*this->_graph, R_level_spread, R_degree_sum);

        return NAN;
    }


	/**
	 * Print the results
	 *
	 * @param f the output file
	 */
	virtual void print_results(FILE* f) {

		fprintf(f, " Num levels | Frequency | Fraction | NZ Fraction | Out Degree \n");
		fprintf(f, "------------+-----------+----------+-------------+------------\n");

		size_t sum = 0, sum1 = 0;
		for (size_t n = 0; n < R_level_spread.size(); n++) sum  += R_level_spread[n];
		for (size_t n = 1; n < R_level_spread.size(); n++) sum1 += R_level_spread[n];

		for (size_t n = 0; n < R_level_spread.size(); n++) {
			fprintf(f, "%11lu | %9d | %8.2lf |",
					n, R_level_spread[n],
					R_level_spread[n] / (double) sum);
			if (n > 0)
				fprintf(f, " %11.2lf", R_level_spread[n] / (double) sum1);
			else
				fprintf(f, " %11s", "-");
			if (R_level_spread[n] > 0) 
				fprintf(f, " | %10.2lf", R_degree_sum[n] / (double) R_level_spread[n]);
			else
				fprintf(f, " | %10s", "-");
			fprintf(f, "\n");
		}
		fprintf(f, "\n");
    }
};

#endif
