/*
 * xs1-split.cpp
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
 * Split an X-Stream Type 1 file into 11 files containing 80% and 10 x 2% of
 * the edges using a uniform distribution.
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

using namespace std;


static const char* SHORT_OPTIONS = "hs:u";

static struct option LONG_OPTIONS[] =
{
	{"help"         , no_argument      , 0, 'h'},
	{"small-files"  , required_argument, 0, 's'},
	{"undirected"   , no_argument      , 0, 'u'},
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
	fprintf(stderr, "Usage: %s [OPTIONS] INPUT_FILE\n\n", p);
	free(s);
	
	fprintf(stderr, "Options:\n");
	fprintf(stderr, "  -h, --help            Show this usage information and exit\n");
	fprintf(stderr, "  -s, --small-files N   Set the number of small files\n");
	fprintf(stderr, "  -u, --undirected      Treat the input file as undirected\n");
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

	srand(time(NULL));

	int n_small_files = 10;
	bool undirected = false;
	double p_large = 0.80;


	// Pase the command-line arguments

	int option_index = 0;
	while (true) {
		int c = getopt_long(argc, argv, SHORT_OPTIONS, LONG_OPTIONS, &option_index);

		if (c == -1) break;

		switch (c) {

			case 'h':
				usage(argv[0]);
				return 0;

			case 's':
				n_small_files = atoi(optarg);
				if (n_small_files <= 0) {
					fprintf(stderr, "Error: Invalid number of small files\n");
					return 1;
				}
				break;

			case 'u':
				undirected = true;
				break;

			case '?':
			case ':':
				return 1;

			default:
				abort();
		}
	}

	double p_small = (1 - p_large) / n_small_files;

	if (optind + 1 != argc) {
		usage(argv[0]);
		return 1;
	}

	const char* src = argv[optind];


	// Do it!
	
	string s_in = src;
	FILE* f_in = fopen(s_in.c_str(), "rb");
	if (f_in == NULL) {
		perror("fopen");
		return 1;
	}
	
	struct stat st;
	int fr = fstat(fileno(f_in), &st);
	if (fr < 0) {
		perror("fstat");
		return 1;
	}
	size_t lines = st.st_size / sizeof(xs1);

	size_t expected_out_counts[1 + n_small_files];
	size_t expected_out_counts_sum = 0;
	expected_out_counts[0] = (size_t) (p_large * lines);
	expected_out_counts_sum = expected_out_counts[0];
	for (int i = 1; i <= n_small_files; i++) {
		expected_out_counts[i] = (size_t) (p_small * lines);
		expected_out_counts_sum += expected_out_counts[i];
	}
	if (expected_out_counts_sum > lines) abort();
	expected_out_counts[0] += lines - expected_out_counts_sum;

	size_t out_counts[1 + n_small_files];
	for (int i = 0; i <= n_small_files; i++) out_counts[i] = 0;
	
	char s_out_base[s_in.length() + 4];
	strcpy(s_out_base, s_in.c_str());
	size_t l = strlen(s_out_base);
	if (l > 4 && strcmp(s_out_base + (l - 4), ".dat") == 0) {
		*(s_out_base + (l -4)) = '\0';
	}
	
	FILE* out_files[1 + n_small_files];
	for (int i = 0; i <= n_small_files; i++) {
		string s = s_out_base;
		char b[16]; sprintf(b, "-part%d.dat", i);
		s += b;
		out_files[i] = fopen(s.c_str(), "wb");
		if (out_files[i] == NULL) {
			perror("fopen");
			return 1;
		}
	}
	
	xs1 x, x2;
	l = 0;
	while (next(f_in, &x)) {
		l++;

		double r = rand() / (double) RAND_MAX;
		int t = r < p_large ? 0 : 1 + (int) ((r - p_large) / (p_small));
		if (t >= 1 + n_small_files) t = n_small_files + 1;
		for (int i = 0; i <= n_small_files; i++) {
			if (out_counts[t] < expected_out_counts[t]) break;
			t = (t + 1) % (1 + n_small_files);
		}

		out_counts[t]++;
		ssize_t q = fwrite(&x, sizeof(xs1), 1, out_files[t]);
		if (q <= 0) {
			perror("fwrite");
			return 1;
		}

		if (undirected) {
			l++;
			if (!next(f_in, &x2)) {
				fprintf(stderr, "Odd number of edges\n");
				return 1;
			}
			if (x2.head != x.tail || x2.tail != x.head) {
				fprintf(stderr, "Not an undirected edge\n");
				return 1;
			}
			out_counts[t]++;
			ssize_t q = fwrite(&x2, sizeof(xs1), 1, out_files[t]);
			if (q <= 0) {
				perror("fwrite");
				return 1;
			}
		}

		if ((l % 10000000ul) == 0) {
			fprintf(stderr, ".");
			if ((l % 100000000ul) == 0) fprintf(stderr, "%lu", l / 1000000ul);
		}
	}
	fprintf(stderr, "\n");

	for (int i = 0; i <= n_small_files; i++) {
		fclose(out_files[i]);
	}
	fclose(f_in);
	return 0;
}

