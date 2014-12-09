/*
 * degree_distribution.h
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


#ifndef LL_DEGREE_DISTRIBUTION_H
#define LL_DEGREE_DISTRIBUTION_H

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
 * Tool: Degree distribution
 */
template <class Graph>
class ll_t_degree_distribution : public ll_benchmark<Graph> {

    std::vector<size_t> log2degree_counts;
    std::vector<size_t> log2degree_sums;
	size_t max_edges;


public:

	/**
	 * Create the tool
	 */
	ll_t_degree_distribution()
		: ll_benchmark<Graph>("[Tool] Degree Distribution") {
		max_edges = 0;
	}


	/**
	 * Destroy the tool
	 */
	virtual ~ll_t_degree_distribution(void) {
	}


	/**
	 * Run the tool
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) {
		Graph& G = *this->_graph;

		for (node_t n = 0; n < G.max_nodes(); n++) {
			size_t d = G.out_degree(n);

			size_t l = 0;
			size_t x = d;
			while (x > 1) { l++; x >>= 1; }

			while (log2degree_counts.size() <= l) log2degree_counts.push_back(0);
			log2degree_counts[l] += 1;

			while (log2degree_sums.size() <= l) log2degree_sums.push_back(0);
			log2degree_sums[l] += d;
			max_edges += d;
		}

        return NAN;
    }


	/**
	 * Print the results
	 *
	 * @param f the output file
	 */
	virtual void print_results(FILE* f) {

		Graph& G = *this->_graph;
		size_t n = G.max_nodes();
		size_t m = max_edges;

		fprintf(f, " Deg >= X |  Deg < X |    Count |  Percent |  Cummul. |  Percent \n");
		fprintf(f, "----------+----------+----------+----------+----------+----------\n");

		size_t cummulative_count = 0;
		for (size_t i = 0; i < log2degree_counts.size(); i++) {
			cummulative_count += log2degree_counts[i];
			fprintf(f, " %8lu | %8lu | %8lu | %8.2lf | %8lu | %8.2lf\n",
					i == 0 ? 0 : (1ul << i), (1ul << (i + 1)) - 1,
					log2degree_counts[i], 100.0 * log2degree_counts[i] / (double) n,
					cummulative_count, 100.0 * cummulative_count / (double) n);
		}
		fprintf(f, "\n");

		fprintf(f, " Deg >= X |  Deg < X | Deg. Sum |  Percent |  Cummul. |  Percent \n");
		fprintf(f, "----------+----------+----------+----------+----------+----------\n");

		size_t cummulative_sum = 0;
		for (size_t i = 0; i < log2degree_sums.size(); i++) {
			cummulative_sum += log2degree_sums[i];
			fprintf(f, " %8lu | %8lu | %8lu | %8.2lf | %8lu | %8.2lf\n",
					i == 0 ? 0 : (1ul << i), (1ul << (i + 1)) - 1,
					log2degree_sums[i], 100.0 * log2degree_sums[i] / (double) m,
					cummulative_sum, 100.0 * cummulative_sum / (double) m);
		}
		fprintf(f, "\n");
    }
};

#endif
