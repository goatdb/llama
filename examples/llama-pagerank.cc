/*
 * llama-pagerank.cc
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

/**
 * LLAMA Example: PageRank using persistent storage (LL_PERSISTENCE)
 */

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <getopt.h>
#include <libgen.h>

#include <llama.h>


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

static const char* SHORT_OPTIONS = "d:ht:v";

static struct option LONG_OPTIONS[] =
{
	{"database"     , required_argument, 0, 'd'},
	{"help"         , no_argument,       0, 'h'},
	{"threads"      , required_argument, 0, 't'},
	{"verbose"      , no_argument,       0, 'v'},
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
	fprintf(stderr, "Usage: %s [OPTIONS]\n\n", p);
	free(s);
	
	fprintf(stderr, "Options:\n");
	fprintf(stderr, "  -d, --database DIR    Set the database directory\n");
	fprintf(stderr, "  -h, --help            Show this usage information and exit\n");
	fprintf(stderr, "  -t, --threads N       Set the number of threads\n");
	fprintf(stderr, "  -v, --verbose         Enable verbose output\n");
}


//==========================================================================//
// Utilities                                                                //
//==========================================================================//

struct node_and_data {
	node_t node;
	double data;
};


/**
 * Compare by the node data
 */
int cmp_by_data_desc(const void* a, const void* b) {

	const node_and_data* A = (const node_and_data*) a;
	const node_and_data* B = (const node_and_data*) b;

	if (A->data > B->data) return -1;
	if (A->data < B->data) return  1;
	return (int) A->node - (int) B->node;
}


//==========================================================================//
// The Main Function                                                        //
//==========================================================================//

/**
 * The main function
 */
int main(int argc, char** argv)
{
	char* database_directory = NULL;
	bool verbose = false;
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

			case 'd':
				database_directory = optarg;
				break;

			case 'h':
				usage(argv[0]);
				return 0;

			case 't':
				num_threads = atoi(optarg);
				break;

			case 'v':
				verbose = true;
				break;

			case '?':
			case ':':
				return 1;

			default:
				abort();
		}
	}

	if (optind != argc) {
		fprintf(stderr, "Error: Too many command line arguments\n");
		return 1;
	}


	// Open the database
	
	ll_database database(database_directory);
	if (num_threads > 0) database.set_num_threads(num_threads);
	ll_writable_graph& graph = *database.graph();

	if (!graph.ro_graph().has_reverse_edges()) {
		fprintf(stderr, "Error: The graph does not have reverse edges\n");
		return 1;
	}


	// Run the computation

	double ts = ll_get_time_ms();
	double* pr = pagerank(graph.ro_graph(), damping, threshold,
			max_iterations, verbose);
	double t = ll_get_time_ms() - ts;

	if (verbose) {
		fprintf(stderr, "Finished in %3.2lf seconds.\n\n", t/1000.0);
	}


	// Print the top nodes
	
	size_t n = graph.max_nodes();
	node_and_data* a = (node_and_data*) malloc(sizeof(node_and_data) * n);
	for (size_t i = 0; i < n; i++) {
		a[i].node = i;
		a[i].data = pr[i];
	}

	qsort(a, n, sizeof(node_and_data), cmp_by_data_desc);

	size_t max = 10;
	if (max > n) max = n;

	printf("\tNode\tPageRank\n");
	for (size_t i = 0; i < max; i++) {
		printf("%lu\t%lu\t%0.6lf\n", i+1, a[i].node, a[i].data);
	}

	free(a);
	free(pr);

	return 0;
}
