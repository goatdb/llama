/*
 * llama-load.cc
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


#include <sys/time.h>
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

#include <llama.h>


//==========================================================================//
// The Command-Line Arguments                                               //
//==========================================================================//

static const char* SHORT_OPTIONS = "d:DhIT:t:UOvX:";

static struct option LONG_OPTIONS[] =
{
	{"database"     , required_argument, 0, 'd'},
	{"deduplicate"  , no_argument      , 0, 'D'},
	{"help"         , no_argument,       0, 'h'},
	{"in-edges"     , no_argument,       0, 'I'},
	{"temp-dir"     , required_argument, 0, 'T'},
	{"threads"      , required_argument, 0, 't'},
	{"undir-double" , no_argument,       0, 'U'},
	{"undir-order"  , no_argument,       0, 'O'},
	{"verbose"      , no_argument,       0, 'v'},
	{"xs-buffer"    , required_argument, 0, 'X'},
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
	fprintf(stderr, "  -d, --database DIR    Set the database directory\n");
	fprintf(stderr, "  -D, --deduplicate     Deduplicate edges within level while loading\n");
	fprintf(stderr, "  -h, --help            Show this usage information and exit\n");
	fprintf(stderr, "  -I, --in-edges        Load or generate in-edges\n");
	fprintf(stderr, "  -O, --undir-order     Load undirected by ordering all edges\n");
	fprintf(stderr, "  -t, --threads N       Set the number of threads\n");
	fprintf(stderr, "  -T, --temp DIR        Add a temporary directory\n");
	fprintf(stderr, "  -U, --undir-double    Load undirected by doubling all edges\n");
	fprintf(stderr, "  -v, --verbose         Enable verbose output\n");
	fprintf(stderr, "  -X, --xs-buffer GB    Set the external sort buffer size, in GB\n");
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

	ll_loader_config loader_config;
	int num_threads = -1;


	// Pase the command-line arguments

	int option_index = 0;
	while (true) {
		int c = getopt_long(argc, argv, SHORT_OPTIONS, LONG_OPTIONS, &option_index);

		if (c == -1) break;

		switch (c) {

			case 'd':
				database_directory = optarg;
				break;

			case 'D':
				loader_config.lc_deduplicate = true;
				break;

			case 'h':
				usage(argv[0]);
				return 0;

			case 'I':
				loader_config.lc_reverse_edges = true;
				loader_config.lc_reverse_maps  = true;
				break;

			case 'O':
				loader_config.lc_direction = LL_L_UNDIRECTED_ORDERED;
				break;

			case 't':
				num_threads = atoi(optarg);
				break;

			case 'T':
				loader_config.lc_tmp_dirs.push_back(optarg);
				break;

			case 'U':
				loader_config.lc_direction = LL_L_UNDIRECTED_DOUBLE;
				break;

			case 'v':
				verbose = true;
				loader_config.lc_print_progress = true;
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

	std::vector<std::string> input_files;
	for (int i = optind; i < argc; i++) {
		input_files.push_back(std::string(argv[i]));
	}

	if (database_directory == NULL) {
		database_directory = (char*) alloca(16);
		strcpy(database_directory, "db");
	}


	// Check the input files

	if (input_files.empty()) {
		fprintf(stderr, "Error: No input files are specified\n");
		return 1;
	}

	const char* first_input = input_files[0].c_str();
	const char* file_type = ll_file_extension(first_input);

	for (size_t i = 1; i < input_files.size(); i++) {
		if (strcmp(ll_file_extension(input_files[i].c_str()), file_type)!=0) {
			fprintf(stderr, "Error: All imput files must have the same "
					"file extension.\n");
			return 1;
		}
	}


	// Open the database
	
	ll_database database(database_directory);
	if (num_threads > 0) database.set_num_threads(num_threads);
	ll_writable_graph& graph = *database.graph();


	// Load the graph

	ll_file_loaders loaders;
	ll_file_loader* loader = loaders.loader_for(first_input);
	if (loader == NULL) {
		fprintf(stderr, "Error: Unsupported input file type\n");
		return 1;
	}

	if (verbose) fprintf(stderr, "Loading:\n");
	for (size_t i = 0; i < input_files.size(); i++) {
		if (verbose) fprintf(stderr, " %2lu: %s", i+1, input_files[i].c_str());

		double ts = ll_get_time_ms();
		loader->load_direct(&graph, input_files[i].c_str(), &loader_config);
		double t = ll_get_time_ms() - ts;

		if (verbose) {
			fprintf(stderr, " (Load: %3.2lf s, %7.2lf Kedges/s)\n",
					t/1000.0, graph.max_edges(graph.num_levels() - 2) / t);
		}
	}

	return 0;
}
