/*
 * llama-frappe.cc
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

#include <readline/readline.h>
#include <readline/history.h>

#include <llama.h>


//==========================================================================//
// The Command-Line Arguments                                               //
//==========================================================================//

static const char* SHORT_OPTIONS = "DhT:t:vX:";

static struct option LONG_OPTIONS[] =
{
	{"deduplicate"  , no_argument      , 0, 'D'},
	{"help"         , no_argument,       0, 'h'},
	{"temp-dir"     , required_argument, 0, 'T'},
	{"threads"      , required_argument, 0, 't'},
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
	fprintf(stderr, "  -D, --deduplicate     Deduplicate edges within level while loading\n");
	fprintf(stderr, "  -h, --help            Show this usage information and exit\n");
	fprintf(stderr, "  -t, --threads N       Set the number of threads\n");
	fprintf(stderr, "  -T, --temp DIR        Add a temporary directory\n");
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
	bool verbose = false;
	int num_threads = -1;

	ll_loader_config loader_config;
	loader_config.lc_reverse_edges = true;
	loader_config.lc_reverse_maps  = true;


	// Pase the command-line arguments

	int option_index = 0;
	while (true) {
		int c = getopt_long(argc, argv, SHORT_OPTIONS, LONG_OPTIONS, &option_index);

		if (c == -1) break;

		switch (c) {

			case 'D':
				loader_config.lc_deduplicate = true;
				break;

			case 'h':
				usage(argv[0]);
				return 0;

			case 't':
				num_threads = atoi(optarg);
				break;

			case 'T':
				loader_config.lc_tmp_dirs.push_back(optarg);
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
	
	ll_database database("");
	if (num_threads > 0) database.set_num_threads(num_threads);
	ll_writable_graph& graph = *database.graph();
	ll_mlcsr_ro_graph& g = graph.ro_graph();


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


	// Initialize
	
	rl_bind_key('\t', rl_abort);

	ll_mlcsr_node_property<std::string*>* _name
		= reinterpret_cast<ll_mlcsr_node_property<std::string*>*>(
				graph.ro_graph().get_node_property_64("NAME"));
	if (_name == NULL) {
		fprintf(stderr, "No NAME property\n");
		return 1;
	}
	ll_mlcsr_node_property<std::string*>& name = *_name;


	ll_mlcsr_edge_property<std::string*>* _label
		= reinterpret_cast<ll_mlcsr_edge_property<std::string*>*>(
				graph.ro_graph().get_edge_property_64("label"));
	if (_label == NULL) {
		fprintf(stderr, "No label property\n");
		return 1;
	}
	ll_mlcsr_edge_property<std::string*>& label = *_label;

	int* dist[2];
	dist[0] = (int*) malloc(graph.max_nodes() * sizeof(int));
	dist[1] = (int*) malloc(graph.max_nodes() * sizeof(int));

	node_t* prev[2];
	prev[0] = (node_t*) malloc(graph.max_nodes() * sizeof(node_t));
	prev[1] = (node_t*) malloc(graph.max_nodes() * sizeof(node_t));


	// The main loop

	fprintf(stderr, "\nCommands:\n");
	fprintf(stderr, "  find      Find symbol by name\n");
	fprintf(stderr, "  sp        Find shortest path\n");
	fprintf(stderr, "  usedby    Find which symbols the given symbol uses\n");
	fprintf(stderr, "  usedin    Find in which symbols the given symbol is used\n");
	fprintf(stderr, "  usp       Find undirected shortest path\n");
	fprintf(stderr, "  q         quit\n");
	fprintf(stderr, "\n");

	char* buf;
	while ((buf = readline(">> ")) != NULL) {

		if (strcmp(buf, "q") == 0) break;
		if (*buf == '\0') continue;
		
		add_history(buf);


		// Tokenize

		char* token = strtok(buf, " \t");
		   
		std::vector<std::string> tokens; 
		while (token != NULL) {
			tokens.push_back(std::string(token));
			token = strtok(NULL, " \t");
		}


		// Command

		double t_start = ll_get_time_ms();
		
		if (tokens[0] == "find") {
			if (tokens.size() != 2) {
				fprintf(stderr, "Usage: %s SYMBOL\n", tokens[0].c_str());
				continue;
			}
			for (node_t i = 0; i < g.max_nodes(); i++) {
				std::string* s = name[i];
				if (s != NULL && *s == tokens[1]) {
					fprintf(stdout, "Vertex %ld\n", i);
				}
			}
		}
		
		else if (tokens[0] == "sp" || tokens[0] == "usp") {
			if (tokens.size() < 3 || tokens.size() > 4) {
				fprintf(stderr, "Usage: %s VERTEX_ID|SYMBOL VERTEX_ID|SYMBOL [EDGE_LABEL]\n", tokens[0].c_str());
				continue;
			}

			bool undirected = tokens[0] == "usp";

			char* end;
			const char* edge_label = tokens.size() >= 4 ? tokens[3].c_str() : NULL;

			node_t n1 = strtol(tokens[1].c_str(), &end, 10);
			if (end == tokens[1].c_str()) {
				n1 = LL_NIL_NODE;
				for (node_t i = 0; i < g.max_nodes(); i++) {
					std::string* s = name[i];
					if (s != NULL && *s == tokens[1]) {
						n1 = i;
						break;
					}
				}
				if (n1 == LL_NIL_NODE) {
					fprintf(stderr, "Cannot find the symbol\n");
					continue;
				}
			}
			else {
				if (n1 < 0 || n1 >= g.max_nodes()) {
					fprintf(stderr, "Invalid vertex ID\n");
					continue;
				}
			}

			node_t n2 = strtol(tokens[2].c_str(), &end, 10);
			if (end == tokens[2].c_str()) {
				n2 = LL_NIL_NODE;
				for (node_t i = 0; i < g.max_nodes(); i++) {
					std::string* s = name[i];
					if (s != NULL && *s == tokens[2]) {
						n2 = i;
						break;
					}
				}
				if (n2 == LL_NIL_NODE) {
					fprintf(stderr, "Cannot find the symbol\n");
					continue;
				}
			}
			else {
				if (n2 < 0 || n2 >= g.max_nodes()) {
					fprintf(stderr, "Invalid vertex ID\n");
					continue;
				}
			}


			// The following was adapted from the code by Albert Wu and David
			// Ding of Harvard. Original license was BSD.
			
			std::deque<node_t> queues[2];
			queues[0].push_back(n1);
			queues[1].push_back(n2);
			bool found = false;
			unsigned int distance = UINT_MAX;

#			pragma omp parallel for
			for (node_t t = 0; t < g.max_nodes(); t++) {
				dist[0][t] = -1;
				dist[1][t] = -1;
			}

			dist[0][n1] = 0;
			dist[1][n2] = 0;

			prev[0][n1] = LL_NIL_NODE;
			prev[1][n2] = LL_NIL_NODE;

			size_t processed[2];
			processed[0] = 0;
			processed[1] = 0;

			while (!(queues[0].empty() && queues[1].empty()) && !found) {
				for (int j = 0; !found && j < 2; j++) {
					if (queues[j].empty()) continue;

					node_t node = queues[j].front();
					queues[j].pop_front();
					ll_edge_iterator it;
					memset(&it, 0, sizeof(it));
					edge_t e;
					processed[j]++;

					size_t p = processed[0] + processed[1];
					if (verbose && p % 1000000 == 0) {
						fprintf(stderr, "Processed %lu vertices so far...\n", p);
					}

					for (int d = 0; !found && d < (undirected ? 2 : 1); d++) {
						
						if (!undirected) d = j;
						//std::string* ns = name[node];
						//fprintf(stderr, "%d%d %8ld %s\n", d, undirected, node, ns == NULL ? "" : ns->c_str());

						if (d == 0) {
							g.out_iter_begin(it, node);
						}
						else {
							g.in_iter_begin_fast(it, node);
						}

						for (e = d ? g.in_iter_next_fast(it) : g.out_iter_next(it);
								e != LL_NIL_EDGE && !found; 
								e = d ? g.in_iter_next_fast(it) : g.out_iter_next(it)) {
							node_t next = it.last_node;
							if (dist[j][next] != -1) continue;

							if (edge_label != NULL) {
								edge_t out_edge = d ? g.in_to_out(e) : e;
								std::string* l = label[out_edge];
								if (l == NULL) continue;
								if (*l != edge_label) continue;
							}

							dist[j][next] = dist[j][node]+1;
							prev[j][next] = node;

							if (dist[1-j][next] != -1) {
								if (!found) {
									found = true;
									prev[1-j][node] = next;
									distance = dist[1-j][next] + dist[j][node] + 1;

									if (j) std::swap(node, next);

									std::deque<node_t> path;
									node_t n = node;
									path.push_back(n);
									while (n != n1) {
										n = prev[0][n];
										path.push_front(n);
									}
									n = next;
									path.push_back(n);
									while (n != n2) {
										n = prev[1][n];
										path.push_back(n);
									}

									for (size_t i = 0; i < path.size(); i++) {
										node_t n = path[i];
										std::string* s = name[n];
										fprintf(stderr, "%8ld %s\n", n, s == NULL ? "" : s->c_str());
									}

									fprintf(stderr, "Distance: %u\n", distance);
									break;
								}
							}

							queues[j].push_back(next);
						}
					}
				}
			}

			if (!found) {
				fprintf(stderr, "Path not found\n");
			}
		}
		
		else if (tokens[0] == "usedby") {
			if (tokens.size() != 2) {
				fprintf(stderr, "Usage: %s VERTEX_ID|SYMBOL\n", tokens[0].c_str());
				continue;
			}

			char* end;
			node_t n = strtol(tokens[1].c_str(), &end, 10);
			if (end == tokens[1].c_str()) {
				n = LL_NIL_NODE;
				for (node_t i = 0; i < g.max_nodes(); i++) {
					std::string* s = name[i];
					if (s != NULL && *s == tokens[1]) {
						n = i;
						break;
					}
				}
				if (n == LL_NIL_NODE) {
					fprintf(stderr, "Cannot find the symbol\n");
					continue;
				}
			}
			else {
				if (n < 0 || n >= g.max_nodes()) {
					fprintf(stderr, "Invalid vertex ID\n");
					continue;
				}
			}

			ll_foreach_out_ext(e, t, g, n) {
				std::string* s = name[t];
				std::string* l = label[e];
				fprintf(stdout, "[%16s] %8ld %s\n",
						l == NULL ? "" : l->c_str(), t, s == NULL ? "" : s->c_str());
			}
		}
		
		else if (tokens[0] == "usedin") {
			if (tokens.size() != 2) {
				fprintf(stderr, "Usage: %s VERTEX_ID|SYMBOL\n", tokens[0].c_str());
				continue;
			}

			char* end;
			node_t n = strtol(tokens[1].c_str(), &end, 10);
			if (end == tokens[1].c_str()) {
				n = LL_NIL_NODE;
				for (node_t i = 0; i < g.max_nodes(); i++) {
					std::string* s = name[i];
					if (s != NULL && *s == tokens[1]) {
						n = i;
						break;
					}
				}
				if (n == LL_NIL_NODE) {
					fprintf(stderr, "Cannot find the symbol\n");
					continue;
				}
			}
			else {
				if (n < 0 || n >= g.max_nodes()) {
					fprintf(stderr, "Invalid vertex ID\n");
					continue;
				}
			}

			ll_foreach_in_ext(e, t, g, n) {
				std::string* s = name[t];
				std::string* l = label[e];
				fprintf(stdout, "[%16s] %8ld %s\n",
						l == NULL ? "" : l->c_str(), t, s == NULL ? "" : s->c_str());
			}
		}

		else {
			fprintf(stderr, "Unrecognized command\n");
			continue;
		}

		double t = ll_get_time_ms() - t_start;
		fprintf(stderr, "(%0.3lf secodns)\n", t / 1000.0);
	}

	free(buf);

	return 0;
}
