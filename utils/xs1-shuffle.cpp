/*
 * xs1-shuffle.cpp
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
 * LLAMA Utilities -- X-Stream tools
 *
 * Shuffle the order of edges in an X-Stream Type 1 file.
 *
 * DISCLAIMER: This tool was developed separately from X-Stream and without
 * their endorsement.
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <getopt.h>
#include <libgen.h>
#include <string>
#include <unistd.h>

#include "xs1-common.h"
#include "llama/ll_external_sort.h"

using namespace std;


// Comnmand-line arguments

static const char* SHORT_OPTIONS = "hM:S:T:X:";

static struct option LONG_OPTIONS[] =
{
	{"help"         , no_argument      , 0, 'h'},
	{"max-edges"    , required_argument, 0, 'M'},
	{"seed"         , required_argument, 0, 'S'},
	{"temp-dir"     , required_argument, 0, 'T'},
	{"xs-buffer"    , required_argument, 0, 'X'},
	{0, 0, 0, 0}
};


/**
 * xs1 + a random key
 */
struct xs1r {
	struct xs1 payload;
	int random1;
	int random2;
};


/**
 * Comparator for edge
 */
struct xs1r_edge_comparator {
	bool operator() (const xs1r& a, const xs1r& b) {
		if (a.random1 != b.random1) return a.random1 < b.random1;
		if (a.random2 != b.random2) return a.random2 < b.random2;
		if (a.payload.tail != b.payload.tail) return a.payload.tail < b.payload.tail;
		return a.payload.head < b.payload.head;
	}
};


/**
 * Print the usage information
 *
 * @param arg0 the first element in the argv array
 */
static void usage(const char* arg0) {

	char* s = strdup(arg0);
	char* p = basename(s);
	fprintf(stderr, "Usage: %s [OPTIONS] INPUT_FILE OUTPUT_FILE\n\n", p);
	free(s);
	
	fprintf(stderr, "Options:\n");
	fprintf(stderr, "  -h, --help            Show this usage information and exit\n");
	fprintf(stderr, "  -M, --max-edges N     Set maximum number of edges to read\n");
	fprintf(stderr, "  -S, --seed SEED       Set the random number generator seed\n");
	fprintf(stderr, "  -T, --temp DIR        Set the temporary directory\n");
	fprintf(stderr, "  -X, --xs-buffer GB    Set the external sort buffer size, in GB\n");
	fprintf(stderr, "\n");
}


/**
 * The main function
 *
 * @param argc the number of command-line arguments
 * @param argv the command-line arguments
 * @return the exit code
 */
int main(int argc, char** argv) {

	bool progress = true;
	unsigned progress_step = 10000000;
	int seed = time(NULL);

	ll_loader_config loader_config;
	size_t max_edges = 0;


	// Pase the command-line arguments

	int option_index = 0;
	while (true) {
		int c = getopt_long(argc, argv, SHORT_OPTIONS, LONG_OPTIONS, &option_index);

		if (c == -1) break;

		switch (c) {

			case 'h':
				usage(argv[0]);
				return 0;

			case 'M':
				max_edges = atol(optarg);
				break;

			case 'S':
				seed = atoi(optarg);
				break;

			case 'T':
				if (loader_config.lc_tmp_dirs.empty())
					loader_config.lc_tmp_dirs.push_back(optarg);
				else
					loader_config.lc_tmp_dirs[0] = optarg;
				break;

			case 'X':
				loader_config.lc_xs_buffer_size = (size_t) (atof(optarg)
						* 1024ul*1048576ul);
				break;

			case '?':
			case ':':
				return 1;

			default:
				abort();
		}
	}

	if (optind + 2 != argc) {
		usage(argv[0]);
		return 1;
	}

	srand(seed);

	const char* src = argv[optind];
	const char* dst = argv[optind+1];


	// Do it!
	
	string s_in = src;
	FILE* f_in = fopen(s_in.c_str(), "rb");
	if (f_in == NULL) {
		perror("fopen");
		return 1;
	}


	// Get the number of edges
	
	struct stat st;
	int fr = fstat(fileno(f_in), &st);
	if (fr < 0) {
		perror("fstat");
		return 1;
	}
	size_t num_edges = st.st_size / sizeof(xs1);

	if (num_edges == 0) {
		fprintf(stderr, "The input file is empty");
		fclose(f_in);
		return 1;
	}


	// Load the edges into an external "sort" (i.e. a shuffler)

	if (progress) fprintf(stderr, "Loading the edges:\n  ");

	ll_external_sort<xs1r, xs1r_edge_comparator> external_shuffle(&loader_config);

	unsigned l = 0;
	xs1r r;
	while (next(f_in, &r.payload)) {
		l++;

		if (max_edges > 0 && l > max_edges) break;

		r.random1 = rand();
		r.random2 = rand();
		external_shuffle << r;

		if (progress && (l % progress_step) == 0) {
			fprintf(stderr, ".");
			if ((l % (progress_step * 10)) == 0) {
				fprintf(stderr, "%um", l / 1000000);
			}
			if ((l % (progress_step * 50)) == 0) {
				fprintf(stderr, "\n  ");
			}
		}
	}
	if (progress) {
		if ((l % (progress_step * 50)) >= progress_step) fprintf(stderr, "\n  ");
		fprintf(stderr, "Done.\n");
	}


	// Save

	external_shuffle.sort();

	if (progress) fprintf(stderr, "Saving the edges:\n  ");
	
	string s_out = dst;
	FILE* f_out = fopen(s_out.c_str(), "wb");
	if (f_out == NULL) {
		perror("fopen");
		return 1;
	}

	xs1r* buffer;
	size_t length;

	l = 0;
	while (external_shuffle.next_block(&buffer, &length)) {
		while (length --> 0) {

			ssize_t r = fwrite(&buffer->payload, sizeof(xs1), 1, f_out);
			if (r <= 0) {
				if (r < 0) {
					perror("fwrite");
				}
				else {
					fprintf(stderr, "Out of space\n");
				}
				fclose(f_in);
				fclose(f_out);
				return 1;
			}

			buffer++;
			l++;

			if (progress && (l % progress_step) == 0) {
				fprintf(stderr, ".");
				if ((l % (progress_step * 10)) == 0) {
					fprintf(stderr, "%um", l / 1000000);
				}
				if ((l % (progress_step * 50)) == 0) {
					fprintf(stderr, "\n  ");
				}
			}
		}
	}
	if (progress) {
		if ((l % (progress_step * 50)) >= progress_step) fprintf(stderr, "\n  ");
		fprintf(stderr, "Done.\n");
	}


	// Finalize

	if (f_out) fclose(f_out);
	if (f_in) fclose(f_in);

	return 0;
}

