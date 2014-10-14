/*
 * snap-to-xs1.cpp
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
 * Convert a SNAP file to an X-Stream Type 1 file
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


static const char* SHORT_OPTIONS = "ho:uS:";

static struct option LONG_OPTIONS[] =
{
	{"help"         , no_argument      , 0, 'h'},
	{"output"       , required_argument, 0, 'o'},
	{"undirected"   , no_argument      , 0, 'u'},
	{"seed"         , required_argument, 0, 'S'},
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
	fprintf(stderr, "  -o, --output FILE     Set the output file\n");
	fprintf(stderr, "  -S, --seed N          Set the random number generator seed\n");
	fprintf(stderr, "  -u, --undirected      Generate an undirected output file\n");
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

	bool undirected = false;
	std::string output = "";

	bool progress = true;
	size_t progress_step = 10000000;


	// Pase the command-line arguments

	int option_index = 0;
	while (true) {
		int c = getopt_long(argc, argv, SHORT_OPTIONS, LONG_OPTIONS, &option_index);

		if (c == -1) break;

		switch (c) {

			case 'h':
				usage(argv[0]);
				return 0;

			case 'o':
				output = optarg;
				break;

			case 'S':
				srand(atoi(optarg));
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

	if (optind + 1 != argc) {
		usage(argv[0]);
		return 1;
	}

	const char* src = argv[optind];

	if (output == "") {
		const char* s = strrchr(src, '.');
		if (s != NULL && (strcmp(s, ".net") == 0 || strcmp(s, ".txt") == 0
				|| strcmp(s, ".snap") == 0)) {
			char* p = strdup(src);
			*(p + (s - src)) = '\0';
			output = p;
			free(p);
		}
		else {
			output = src;
		}
		output += ".dat";
	}


	// Do it!
	
	FILE* f_in = fopen(src, "rb");
	if (f_in == NULL) {
		perror("fopen");
		return 1;
	}
	
	FILE* f_out = output == "-" ? stdout : fopen(output.c_str(), "wb");
	if (f_out == NULL) {
		perror("fopen");
		fclose(f_in);
		return 1;
	}

	ssize_t read;
	size_t num_nodes = 0;
	size_t num_edges = 0;
	size_t num_lines = 0;

	size_t line_len = 64;
	char* line = (char*) malloc(line_len);

	while ((read = getline(&line, &line_len, f_in)) != -1) {

		num_lines++;

		if (*line == '\0' || *line == '#'
				|| *line == '\n' || *line == '\r') continue;

		size_t ln = strlen(line)-1;
		if (line[ln] == '\n' || line[ln] == '\r') line[ln] = '\0';

		if (!isdigit(*line)) {
			fprintf(stderr, "Invalid .net format on line \"%s\"\n", line);
			abort();
		}

		char* l = line;
		while (isdigit(*l)) l++;
		if (*l == '\0') {
			fprintf(stderr, "Invalid .net format on line \"%s\"\n", line);
			abort();
		}

		while (isspace(*l)) l++;
		if (*l == '\0' || !isdigit(*l)) {
			fprintf(stderr, "Invalid .net format on line \"%s\"\n", line);
			abort();
		}

		num_edges++;

		xs1 x;
		x.tail = atoi(line);
		x.head = atoi(l);
		x.weight = rand() / (float) RAND_MAX;

		if (x.head >= num_nodes) num_nodes = x.head + 1;
		if (x.tail >= num_nodes) num_nodes = x.tail + 1;

		ssize_t r = fwrite(&x, sizeof(xs1), 1, f_out);
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

		if (undirected) {

			unsigned t = x.tail; x.tail = x.head; x.head = t;
			num_edges++;

			ssize_t r = fwrite(&x, sizeof(xs1), 1, f_out);
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
		}

		if (progress && (num_lines % progress_step) == 0) {
			fprintf(stderr, ".");
			if ((num_lines % (progress_step * 10)) == 0) {
				fprintf(stderr, "%lum", num_lines / 1000000);
			}
			if ((num_lines % (progress_step * 50)) == 0) {
				fprintf(stderr, "\n");
			}
		}
	}

	if (progress) {
		if ((num_lines % (progress_step * 50)) >= progress_step) {
			fprintf(stderr, "\n");
		}
	}

	if (f_out != stdout && f_out != stderr) {
		fclose(f_out);

		if (!write_ini(output.c_str(), num_nodes, num_edges)) {
			perror("write_ini");
			return 1;
		}
	}

	fclose(f_in);
	free(line);

	return 0;
}

