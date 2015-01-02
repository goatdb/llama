/*
 * xs1-add-duplicates.cpp
 * LLAMA Graph Analytics
 *
 * Copyright 2015
 *      The President and Fellows of Harvard College.
 *
 * Copyright 2015
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
 * Add duplicate edges to an X-Stream Type 1 file.
 *
 * DISCLAIMER: This tool was developed separately from X-Stream and without
 * their endorsement.
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <getopt.h>
#include <libgen.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "xs1-common.h"

using namespace std;


// Comnmand-line arguments

static const char* SHORT_OPTIONS = "hA:M:p:S:W:";

static struct option LONG_OPTIONS[] =
{
	{"advance"      , required_argument, 0, 'A'},
	{"help"         , no_argument      , 0, 'h'},
	{"max-edges"    , required_argument, 0, 'M'},
	{"probability"  , required_argument, 0, 'p'},
	{"seed"         , required_argument, 0, 'S'},
	{"window"       , required_argument, 0, 'W'},
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
	fprintf(stderr, "Usage: %s [OPTIONS] INPUT_FILE OUTPUT_FILE\n\n", p);
	free(s);
	
	fprintf(stderr, "Options:\n");
	fprintf(stderr, "  -A, --advance N       The number of edges to advance by\n");
	fprintf(stderr, "  -h, --help            Show this usage information and exit\n");
	fprintf(stderr, "  -M, --max-edges N     Set maximum number of edges to write\n");
	fprintf(stderr, "  -p, --probability P   The probability of choosing a duplicate\n");
	fprintf(stderr, "  -S, --seed SEED       Set the random number generator seed\n");
	fprintf(stderr, "  -W, --window N        The number of edges in a window\n");
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

	size_t max_edges = 0;
	size_t advance = 1000;
	size_t window = 0;
	double probability = 0.1;


	// Pase the command-line arguments

	int option_index = 0;
	while (true) {
		int c = getopt_long(argc, argv, SHORT_OPTIONS, LONG_OPTIONS, &option_index);

		if (c == -1) break;

		switch (c) {

			case 'A':
				advance = atol(optarg);
				break;

			case 'h':
				usage(argv[0]);
				return 0;

			case 'M':
				max_edges = atol(optarg);
				break;

			case 'p':
				probability = atof(optarg);
				break;

			case 'S':
				seed = atoi(optarg);
				break;

			case 'W':
				window = atol(optarg);
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

	if (window == 0) {
		fprintf(stderr, "Error: The -W option is required\n");
		return 1;
	}

	if (advance == 0 || window % advance != 0) {
		fprintf(stderr, "Error: The window size must be a multiple of the "
				"advance size\n");
		return 1;
	}

	if (probability < 0 || probability > 1) {
		fprintf(stderr, "Error: The duplicate probability must be between 0 "
				"and 1\n");
		return 1;
	}

	size_t num_buffers = window / advance;

	srand(seed);

	const char* src = argv[optind];
	const char* dst = argv[optind+1];


	// Open the files
	
	string s_in = src;
	FILE* f_in = fopen(s_in.c_str(), "rb");
	if (f_in == NULL) {
		perror("fopen");
		return 1;
	}
	
	string s_out = dst;
	FILE* f_out = fopen(s_out.c_str(), "wb");
	if (f_out == NULL) {
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


	// Do it!

	if (progress) fprintf(stderr, "Processing:\n  ");

	std::deque<std::vector<xs1>*> buffer_queue;

	size_t l = 0;
	size_t written = 0;
	size_t duplicates = 0;
	size_t num_nodes = 0;

	xs1 x;

	while (true) {


		// Load the next chunk

		std::vector<xs1>* buffer = NULL;
		if (buffer_queue.size() < num_buffers) {
			buffer = new std::vector<xs1>();
			buffer_queue.push_back(buffer);
		}
		else {
			buffer = buffer_queue.front();
			buffer->clear();
			buffer_queue.pop_front();
			buffer_queue.push_back(buffer);
		}

		while (next(f_in, &x)) {
			l++;

			buffer->push_back(x);

			if (progress && (l % progress_step) == 0) {
				fprintf(stderr, ".");
				if ((l % (progress_step * 10)) == 0) {
					fprintf(stderr, "%lum", l / 1000000l);
				}
				if ((l % (progress_step * 50)) == 0) {
					fprintf(stderr, "\n  ");
				}
			}

			if (buffer->size() >= advance) break;
		}


		// Get the number of available edges for duplicate selection

		size_t available = 0;
		for (size_t i = 0; i < buffer_queue.size() - 1; i++) {
			available += buffer_queue[i]->size();
		}


		// Write the chunk

		size_t r = 0;

		while (r < buffer->size()) {

			double p = rand() / (double) RAND_MAX;
			if (p < probability && available > 0) {
				
				size_t k = (size_t) ((rand() / (double) RAND_MAX) * available);
				bool found = false;
				
				for (size_t i = 0; i < buffer_queue.size(); i++) {
					if (k >= buffer_queue[i]->size()) {
						k -= buffer_queue[i]->size();
					}
					else {
						found = true;
						x = (*buffer_queue[i])[k];
						duplicates++;
						break;
					}
				}

				if (!found) {
					fprintf(stderr, "Internal error\n");
					abort();
				}
			}
			else {

				x = (*buffer)[r++];
				available++;
			}

			if (max_edges > 0 && written >= max_edges) break;

			if (x.head >= num_nodes) num_nodes = x.head + 1;
			if (x.tail >= num_nodes) num_nodes = x.tail + 1;

			ssize_t y = fwrite(&x, sizeof(xs1), 1, f_out);
			if (y <= 0) {
				if (y < 0) {
					perror("fwrite");
				}
				else {
					fprintf(stderr, "Out of space\n");
				}
				fclose(f_in);
				fclose(f_out);
				return 1;
			}

			written++;
		}


		// Check the ending conditions

		if (max_edges > 0 && written >= max_edges) break;
		if (feof(f_in)) break;
	}


	if (progress) {
		if ((l % (progress_step * 50)) >= progress_step) fprintf(stderr, "\n  ");
		fprintf(stderr, "Done. (%lu edges, %lu duplicates, %0.2lf%% actual "
				"duplicate rate)\n",
				written, duplicates, (100.0 * duplicates) / written);
	}


	// Finalize

	if (f_out) fclose(f_out);
	if (f_in) fclose(f_in);

	if (!write_ini(s_out.c_str(), num_nodes, written)) {
		perror("write_ini");
		return 1;
	}

	while (!buffer_queue.empty()) {
		delete buffer_queue.front();
		buffer_queue.pop_front();
	}

	return 0;
}

