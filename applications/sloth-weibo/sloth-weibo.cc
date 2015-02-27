/*
 * sloth-weibo.cc
 * LLAMA Graph Analytics
 *
 * Copyright 2015
 *      The President and Fellows of Harvard College.
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

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <getopt.h>
#include <libgen.h>

#include <sloth.h>

#include "weibo_data_source.h"


/**
 * Run PageRank
 *
 * @param G the graph
 * @param damping the damping factor
 * @param threshold the threshold
 * @param max_iterations the maximum number of iterations
 * @param verbose true for verbose
 * @return the PageRank of each node
 */
double* pagerank(ll_mlcsr_ro_graph& G, double damping, double threshold,
		size_t max_iterations, bool verbose) {

	double* pr      = (double*) calloc(G.max_nodes(), sizeof(double));
	double* pr_next = (double*) calloc(G.max_nodes(), sizeof(double));

	double N = G.max_nodes();
	ll_foreach_node_omp(n, G) pr[n] = 1.0 / N;

	double diff = 0;
	size_t iteration = 0;

	do {
		diff = 0;

		#pragma omp parallel
		{
			double diff_prv = 0;

			#pragma omp for nowait schedule(dynamic,4096)
			ll_foreach_node(n, G) {

				double t = 0;
				ll_foreach_in(w, G, n) {
					t += pr[w] / (double) G.out_degree(w);
				}

				double val = (1 - damping) / N + damping * t;
				pr_next[n] = val;

				diff_prv += std::abs(val - pr[n]);
			}

			ATOMIC_ADD(&diff, diff_prv);
		}

		ll_foreach_node_omp(n, G) pr[n] = pr_next[n];
		iteration++;

		if (verbose) {
			fprintf(stderr, "Iteration %lu: Diff = %lf\n", iteration,
					diff);
		}
	}
	while ((diff > threshold) && (iteration < max_iterations));

	free(pr_next);
	return pr;
}



//==========================================================================//
// The Command-Line Arguments                                               //
//==========================================================================//

static const char* SHORT_OPTIONS = "ht:";

static struct option LONG_OPTIONS[] =
{
	{"help"         , no_argument,       0, 'h'},
	{"threads"      , required_argument, 0, 't'},
	{0, 0, 0, 0}
};


/**
 * Print the usage information
 *
 * @param arg0 the first element in the argv array
 */
static void usage(const char* arg0) {

	char* s = strdup(arg0);
	char* p = basename(s);
	fprintf(stderr, "Usage: %s [OPTIONS] INPUT_FILE...\n\n", p);
	free(s);
	
	fprintf(stderr, "Options:\n");
	fprintf(stderr, "  -h, --help            Show this usage information and exit\n");
	fprintf(stderr, "  -t, --threads N       Set the number of threads\n");
}



//==========================================================================//
// The Main Function                                                        //
//==========================================================================//

/**
 * The main function
 */
int main(int argc, char** argv)
{
	std::setlocale(LC_ALL, "en_US.utf8");

	int num_threads = -1;

	double damping = 0.85;
	double threshold = 0.000001;
	size_t max_iterations = 10;


	// Pase the command-line arguments

	int option_index = 0;
	while (true) {
		int c = getopt_long(argc, argv, SHORT_OPTIONS, LONG_OPTIONS, &option_index);

		if (c == -1) break;

		switch (c) {

			case 'h':
				usage(argv[0]);
				return 0;

			case 't':
				num_threads = atoi(optarg);
				break;

			case '?':
			case ':':
				return 1;

			default:
				abort();
		}
	}


	// Get the input files and create the data source

	std::vector<std::string> input_files;
	for (int i = optind; i < argc; i++) {
		input_files.push_back(std::string(argv[i]));
	}

	if (input_files.empty()) {
		fprintf(stderr, "Error: No input files are specified\n");
		return 1;
	}

	weibo_data_source_csv data_source(input_files);


	// Test

	for (size_t i = 0; i < 10; i++) {
		const tweet_t* t = data_source.next_input();
		if (t == NULL) break;
		printf("%s: %S\n", t->t_user, t->t_text);
	}
	
	double t_start = ll_get_time_ms();

	size_t max = 10 * 1e+6;
	for (size_t i = 0; i < max; i++) {
		data_source.next_input();
	}

	double dt = ll_get_time_ms() - t_start;
	fprintf(stderr, "Read %lu tweets in %0.3lf seconds (%0.3lf Mt/s)\n",
			max, dt / 1000.0, max / dt / 1000.0);

	return 0;
}
