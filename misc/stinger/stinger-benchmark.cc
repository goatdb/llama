/*
 * stinger-benchmark.cc
 * LLAMA Graph Analytics
 *
 * This tool is not associated or supported by STINGER, its developers, or its
 * contributors unless stated otherwise.
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
 * 3. Neither the name of the University nor the name of its contributors
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


#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/resource.h>

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <float.h>
#include <limits.h>
#include <cmath>
#include <algorithm>
#include <omp.h>
#include <getopt.h>
#include <libgen.h>
#include <deque>

#include <llama/ll_mem_helper.h>
#include <llama/ll_utils.h>


//==========================================================================//
// STINGER                                                                  //
//==========================================================================//

extern "C" {
#define restrict __restrict__
#include <stinger-atomics.h>
#include <stinger-utils.h>
#include <stinger.h>
#include <xmalloc.h>
#undef restrict
}


//==========================================================================//
// The Command-Line Arguments                                               //
//==========================================================================//

static const char* SHORT_OPTIONS = "c:hIt:v";

static struct option LONG_OPTIONS[] =
{
	{"count"        , required_argument, 0, 'c'},
	{"help"         , no_argument,       0, 'h'},
	{"incremental"  , no_argument,       0, 'I'},
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
	fprintf(stderr, "Usage: %s [OPTIONS] INPUT_FILE [INPUT_FILE...]\n\n", p);
	free(s);
	
	fprintf(stderr, "Options:\n");
	fprintf(stderr, "  -c, --count N         Set the number of experiment runs\n");
	fprintf(stderr, "  -h, --help            Show this usage information and exit\n");
	fprintf(stderr, "  -I, --incremental     Use incremental load instead of bulk-load\n");
	fprintf(stderr, "  -t, --threads N       Set the number of threads\n");
	fprintf(stderr, "  -v, --verbose         Enable verbose output\n");
}


//==========================================================================//
// X-Stream Type 1                                                          //
//==========================================================================//

/**
 * The X-Stream Type 1 entry
 */
struct xs1 {
	unsigned tail;
	unsigned head;
	float weight;
};


/**
 * Read the next Type 1 entry
 *
 * @param f the file
 * @param out the output
 * @return true if anything was read
 */
bool xs1_next(FILE* f, xs1* out) {
	ssize_t r = fread(out, sizeof(xs1), 1, f);
	if (r < 0) {
		perror("fread");
		abort();
	}
	return r > 0;
}


/**
 * Load a file
 *
 * @param S the stinger graph
 * @param file the file
 * @param progress true to print progress
 */
void xs1_load_incremental(struct stinger* S, const char* file, bool progress=false) {

	FILE* f = fopen(file, "rb");
	if (f == NULL) {
		perror("fopen");
		abort();
	}
	
	struct stat st;
	int r = fstat(fileno(f), &st);
	if (r < 0) {
		perror("fstat");
		abort();
	}
	size_t total = st.st_size / sizeof(xs1);

	xs1 x;
	time_t t = time(NULL);
	size_t loaded = 0;

	if (progress) fprintf(stderr, "%3lu%%", 0ul);

	while (xs1_next(f, &x)) {
		stinger_insert_edge(S, 0, x.tail, x.head, (int64_t) (x.weight * 1000), t);
		loaded++;

		if (progress && (loaded & 0xfffff) == 0) {
			size_t p = loaded * 100 / total;
			fprintf(stderr, "\b\b\b\b%3lu%%", p);
		}
	}

	if (progress) fprintf(stderr, "\b\b\b\b%3lu%%", 100ul);
	fclose(f);
}


/**
 * Load a file
 *
 * @param S the stinger graph
 * @param file the file
 * @param progress true to print progress
 */
void xs1_load_bulk_csr(struct stinger* S, const char* file, bool progress=false) {

	FILE* f = fopen(file, "rb");
	if (f == NULL) {
		perror("fopen");
		abort();
	}
	
	struct stat st;
	int r = fstat(fileno(f), &st);
	if (r < 0) {
		perror("fstat");
		abort();
	}
	size_t total = st.st_size / sizeof(xs1);

	xs1 x;
	time_t t = time(NULL);
	size_t loaded = 0;

	if (progress) fprintf(stderr, "Phase 1: %3lu%%", 0ul);

	int64_t* off = (int64_t*) xmalloc(sizeof(int64_t) * (STINGER_MAX_LVERTICES+1));
	int64_t* to = (int64_t*) xmalloc(sizeof(int64_t) * total);
	int64_t* weights = (int64_t*) xmalloc(sizeof(int64_t) * total);

	memset(off, 0, sizeof(int64_t) * STINGER_MAX_LVERTICES);


	// CSR Degrees and the vertex table

	while (xs1_next(f, &x)) {
		off[x.tail]++;
		loaded++;
		if (progress && (loaded & 0xfffff) == 0) {
			size_t p = loaded * 100 / total;
			fprintf(stderr, "\b\b\b\b%3lu%%", p);
		}
	}

	size_t nv = STINGER_MAX_LVERTICES;
	while (nv > 0) {
		if (off[nv] == 0) nv--; else break;
	}

	size_t prefix = 0;
	for (size_t i = 0; i <= nv; i++) {
		off[i] += prefix;
		prefix = off[i];
	}


	// CSR Data 

	if (progress) fprintf(stderr, "\b\b\b\b\b\b\b2: %3lu%%", 0ul);

	rewind(f);
	loaded = 0;

	int64_t* p = (int64_t*) xmalloc(sizeof(int64_t) * (STINGER_MAX_LVERTICES+1));
	memset(p, 0, sizeof(int64_t) * STINGER_MAX_LVERTICES);

	while (xs1_next(f, &x)) {
		size_t i = off[x.tail] + p[x.tail]++;
		to[i] = x.head;
		weights[i] = (int64_t) (x.weight * 1000);

		loaded++;
		if (progress && (loaded & 0xfffff) == 0) {
			size_t p = loaded * 100 / total;
			fprintf(stderr, "\b\b\b\b%3lu%%", p);
		}
	}

	xfree(p);


	// STINGER

	if (progress) fprintf(stderr, "\b\b\b\b\b\b\b3:     \b\b\b\b");

	stinger_set_initial_edges(S, nv, 0, off, to, weights, NULL, NULL, t);


	// Finish

	if (progress) fprintf(stderr, "done\n");

	xfree(weights);
	xfree(to);
	xfree(off);

	fclose(f);
}


/**
 * Load a file using the edge-list bulk-loader
 *
 * @param S the stinger graph
 * @param file the file
 * @param progress true to print progress
 */
void xs1_load_bulk_el(struct stinger* S, const char* file, bool progress=false) {

	FILE* f = fopen(file, "rb");
	if (f == NULL) {
		perror("fopen");
		abort();
	}
	
	struct stat st;
	int r = fstat(fileno(f), &st);
	if (r < 0) {
		perror("fstat");
		abort();
	}
	size_t total = st.st_size / sizeof(xs1);

	xs1 x;
	time_t t = time(NULL);
	size_t loaded = 0;
	size_t nv = 0;

	int64_t* el_source = (int64_t*) xmalloc(sizeof(int64_t) * total);
	int64_t* el_target = (int64_t*) xmalloc(sizeof(int64_t) * total);
	int64_t* el_weights = (int64_t*) xmalloc(sizeof(int64_t) * total);

	if (progress) fprintf(stderr, "Phase 1: %3lu%%", 0ul);

	while (xs1_next(f, &x)) {
		el_source[loaded] = x.tail;
		el_target[loaded] = x.head;
		el_weights[loaded] = (int64_t) (x.weight * 1000);

		if (x.tail >= nv) nv = x.tail + 1;
		if (x.head >= nv) nv = x.head + 1;

		loaded++;
		if (progress && (loaded & 0xfffff) == 0) {
			size_t p = loaded * 100 / total;
			fprintf(stderr, "\b\b\b\b%3lu%%", p);
		}
	}

	int64_t* off = (int64_t*) xmalloc(sizeof(int64_t) * (STINGER_MAX_LVERTICES+1));
	int64_t* to = (int64_t*) xmalloc(sizeof(int64_t) * total);
	int64_t* weights = (int64_t*) xmalloc(sizeof(int64_t) * total);


	// STINGER

	if (progress) fprintf(stderr, "\b\b\b\b\b\b\b2:     \b\b\b\b");

	edge_list_to_csr(nv, total, el_source, el_target, el_weights, NULL, NULL,
			to, weights, off, NULL, NULL);

	xfree(el_weights);
	xfree(el_target);
	xfree(el_source);

	stinger_set_initial_edges(S, nv, 0, off, to, weights, NULL, NULL, t);


	// Finish

	if (progress) fprintf(stderr, "done\n");

	xfree(weights);
	xfree(to);
	xfree(off);

	fclose(f);
}



//==========================================================================//
// Benchmaks                                                                //
//==========================================================================//

/**
 * Compute PageRank
 *
 * @param S the STINGER graph
 * @param progress true to print progress
 * @param iterations the number of iterations
 * @param d the damping factor
 * @return the sum of PageRank values
 */
float pagerank_push(struct stinger* S,
		bool progress=false,
		size_t iterations=10,
		float d=0.85f) {

	size_t max_vertex = stinger_max_active_vertex(S);
	float N = (float) max_vertex + 1;

	ll_memory_helper m;
	float* G_pg_rank     = m.allocate<float>(max_vertex + 1);
	float* G_pg_rank_nxt = m.allocate<float>(max_vertex + 1);

	if (progress) fprintf(stderr, "%3lu%%", 0ul);

#	pragma omp parallel for
	for (size_t i = 0; i <= max_vertex; i++)  {
		G_pg_rank[i] = 1 / N;
		G_pg_rank_nxt[i] = 0.0f;
	}

	for (size_t iter = 1; iter <= iterations; iter++) {

#		pragma omp parallel
		{

#			pragma omp for schedule(dynamic,4096)
			for (size_t t = 0; t <= max_vertex; t++) 
			{
				int t_degree = stinger_outdegree(S, t);
				if (t_degree == 0) continue;

				float t_pg_rank = G_pg_rank[t];
				float t_delta = t_pg_rank / t_degree;

				STINGER_FORALL_EDGES_OF_VTX_BEGIN(S, t) {
					ATOMIC_ADD<float>(&G_pg_rank_nxt[STINGER_EDGE_DEST], t_delta);
				} STINGER_FORALL_EDGES_OF_VTX_END();
			}

#			pragma omp for schedule(dynamic,4096)
			for (size_t t = 0; t <= max_vertex; t++) 
			{
				G_pg_rank[t] = (1 - d) / N + d * G_pg_rank_nxt[t];
				G_pg_rank_nxt[t] = 0.0f;
			}
		}

		if (progress) fprintf(stderr, "\b\b\b\b%3lu%%", 100 * iter / iterations);
	}

	float s  = 0;
	float s2 = 0;
	size_t c = 0;
	for (size_t n = 0; n <= max_vertex; n++) {
		s  += G_pg_rank[n];
		s2 += G_pg_rank[n] * G_pg_rank[n];
		c++;
	}

	if (progress) fprintf(stderr, "\b\b\b\b%3lu%%", 100ul);
	return s;
}


//==========================================================================//
// The Main Function                                                        //
//==========================================================================//

/**
 * The main function
 */
int main(int argc, char** argv)
{
	bool streaming = false;
	bool verbose = false;
	bool load_incremental = false;

	int num_threads = -1;
	int count = 1;


	// Pase the command-line arguments

	int option_index = 0;
	while (true) {
		int c = getopt_long(argc, argv, SHORT_OPTIONS, LONG_OPTIONS, &option_index);

		if (c == -1) break;

		switch (c) {

			case 'c':
				count = atoi(optarg);
				break;

			case 'h':
				usage(argv[0]);
				return 0;

			case 'I':
				load_incremental = true;
				break;

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

	std::vector<std::string> input_files;
	for (int i = optind; i < argc; i++) {
		input_files.push_back(std::string(argv[i]));
	}


	// Check the input files

	if (input_files.empty()) {
		fprintf(stderr, "Error: No input files are specified\n");
		return 1;
	}

	const char* first_input = input_files[0].c_str();
	const char* file_type = ll_file_extension(first_input);

	if (strcmp(file_type, "dat") != 0 && strcmp(file_type, "xs1") != 0) {
		fprintf(stderr, "Error: Input files must have the X-Stream Type 1 format\n");
		return 1;
	}

	for (size_t i = 1; i < input_files.size(); i++) {
		if (strcmp(ll_file_extension(input_files[i].c_str()), file_type)!=0) {
			fprintf(stderr, "Error: All imput files must have the same "
					"file extension.\n");
			return 1;
		}
	}


	// Set the number of threads
	
	if (num_threads <= 0) {
		num_threads = omp_get_max_threads();
	}
	omp_set_num_threads(num_threads);


	// Create a new STINGER graph

	if (verbose) {
		fprintf(stderr, "Creating a new instance of STINGER:\n");
		fprintf(stderr, "  Max Vertices   : %ld\n", STINGER_MAX_LVERTICES);
		fprintf(stderr, "  Edge Block Pool: %ld\n", EBPOOL_SIZE);
		fprintf(stderr, "  Edge Block Size: %d edges, %ld bytes\n",
				STINGER_EDGEBLOCKSIZE, (long) sizeof(struct stinger_eb));
		fprintf(stderr, "  Memory Cost    : %0.2lf MB\n",
				(EBPOOL_SIZE * sizeof(struct stinger_eb)) / 1048576.0);
		fprintf(stderr, "\n");
	}
	
	struct stinger * S = stinger_new();


	// Non-streaming
	
	if (!streaming) {
		if (verbose) fprintf(stderr, "Loading:\n");
		double t = ll_get_time_ms();
		for (size_t i = 0; i < input_files.size(); i++) {
			if (verbose) fprintf(stderr, "  %s: ", input_files[i].c_str());
			if (load_incremental)
				xs1_load_incremental(S, input_files[i].c_str(), verbose);
			else
				xs1_load_bulk_el(S, input_files[i].c_str(), verbose);
			if (verbose) fprintf(stderr, "\n");
		}
		double t_load_ms = ll_get_time_ms() - t;
		fprintf(stderr, "Loading time: %0.2lf s (%0.2lf min)\n",
				t_load_ms / 1000, t_load_ms / 60000);
		if (count > 1) fprintf(stderr, "\n");

		if (verbose || count == 1) {
			fprintf(stderr, "Computing:%s", count > 1 ? "\n" : " ");
		}
		for (int i = 1; i <= count; i++) {
			if (verbose && count > 1) fprintf(stderr, "  ");
			if (count > 1) fprintf(stderr, "Run %d: ", i);

			t = ll_get_time_ms();
			pagerank_push(S, verbose);
			double t_current_run_ms = ll_get_time_ms() - t;

			if (verbose) fprintf(stderr, "\b\b\b\b");
			fprintf(stderr, "%0.2lf s (%0.2lf min)\n",
					t_current_run_ms / 1000, t_current_run_ms / 60000);
		}
		if (verbose) fprintf(stderr, "\n");
	}


	// Streaming
	
	if (streaming) {

		// TODO
	}


	// Free
	
	S = stinger_free_all(S);
	
	return 0;
}

