/*
 * xs1-reorder.cpp
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
 * Reorder and possibly renumber the vertices and edges in an X-Stream Type 1
 * file, potentially compacting the ID space.
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


// Direction Constants

#define LL_L_DIRECTED				0
#define LL_L_UNDIRECTED_DOUBLE		1
#define LL_L_UNDIRECTED_ORDERED		2


// Comnmand-line arguments

static const char* SHORT_OPTIONS = "CdDhOt:T:X:";

static struct option LONG_OPTIONS[] =
{
	{"compact"      , no_argument      , 0, 'C'},
	{"degree-order" , no_argument      , 0, 'd'},
	{"deduplicate"  , no_argument      , 0, 'D'},
	{"help"         , no_argument      , 0, 'h'},
	{"undir-order"  , no_argument,       0, 'O'},
	{"threads"      , required_argument, 0, 't'},
	{"temp-dir"     , required_argument, 0, 'T'},
	{"xs-buffer"    , required_argument, 0, 'X'},
	{0, 0, 0, 0}
};


/**
 * Node and degree
 */
struct node_and_degree {
	unsigned node;
	int degree;
	bool exists;
};


/**
 * Compare by degree - ascending
 */
int cmp_by_degree_asc(const void* a, const void* b) {
	const node_and_degree* A = (const node_and_degree*) a;
	const node_and_degree* B = (const node_and_degree*) b;
	return A->degree - B->degree;
}


/**
 * Compare by degree - descending
 */
int cmp_by_degree_desc(const void* a, const void* b) {
	const node_and_degree* A = (const node_and_degree*) a;
	const node_and_degree* B = (const node_and_degree*) b;
	return B->degree - A->degree;
}


/**
 * Comparator for edge
 */
struct xs1_edge_comparator {
	bool operator() (const xs1& a, const xs1& b) {
		if (a.tail != b.tail)
			return a.tail < b.tail;
		else
			return a.head < b.head;
	}
};


/**
 * Is g_progress enabled?
 */
bool g_progress = true;


/**
 * The g_progress step
 */
size_t g_progress_step = 10000000;


/**
 * Progress update
 * 
 * @param l the new progress value
 */
void progress_update(unsigned l) {
	if (g_progress && (l % g_progress_step) == 0) {
		fprintf(stderr, ".");
		if ((l % (g_progress_step * 10)) == 0) {
			fprintf(stderr, "%um", l / 1000000);
		}
		if ((l % (g_progress_step * 50)) == 0) {
			fprintf(stderr, "\n  ");
		}
	}
}


/**
 * Progress done
 * 
 * @param l the new progress value
 */
void progress_done(unsigned l) {
	if (g_progress) {
		if ((l % (g_progress_step * 50)) >= g_progress_step)
			fprintf(stderr, "\n  ");
		fprintf(stderr, "Done.\n");
	}
}


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
	fprintf(stderr, "  -C, --compact         Compact the ID space (implied by -d)\n");
	fprintf(stderr, "  -d, --degree-order    Order (renumber) nodes ascending by degree\n");
	fprintf(stderr, "  -D, --deduplicate     Deduplicate edges\n");
	fprintf(stderr, "  -h, --help            Show this usage information and exit\n");
	fprintf(stderr, "  -O, --undir-order     Make undirected by ordering all edges\n");
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

	srand(time(NULL));

	int direction = LL_L_DIRECTED;
	bool deduplicate = false;
	bool degree_order = false;
	bool compact = false;

	ll_loader_config loader_config;

	int num_threads = -1;


	// Pase the command-line arguments

	int option_index = 0;
	while (true) {
		int c = getopt_long(argc, argv, SHORT_OPTIONS, LONG_OPTIONS,
				&option_index);

		if (c == -1) break;

		switch (c) {

			case 'h':
				usage(argv[0]);
				return 0;

			case 'C':
				compact = true;
				break;

			case 'd':
				degree_order = true;
				break;

			case 'D':
				deduplicate = true;
				break;

			case 'O':
				direction = LL_L_UNDIRECTED_ORDERED;
				break;

			case 't':
				num_threads = atoi(optarg);
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

	if (num_threads > 0) {
		omp_set_num_threads(num_threads);
	}

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


	// Deal with the degree order

	xs1 x;
	unsigned n = 0;
	size_t l = 0;
	unsigned* node_map = NULL;

	if (compact || degree_order) {

		size_t capacity = 80 * 1000 * 1000;
		node_and_degree* degrees
			= (node_and_degree*) malloc(sizeof(node_and_degree) * capacity);
		memset(degrees, 0, sizeof(node_and_degree) * capacity);
		for (unsigned i = 0; i < capacity; i++) degrees[i].node = i;

		if (!deduplicate || (!degree_order)) {

			if (g_progress)
				fprintf(stderr, "Computing node degrees:\n  ");

			while (next(f_in, &x)) {
				l++;

				if (x.head >= n) n = x.head;
				if (x.tail >= n) n = x.tail;
				if (n >= capacity) {
					size_t c = capacity;
					while (c <= n + 64) c *= 2;
					degrees = (node_and_degree*)
						realloc(degrees, sizeof(node_and_degree) * c);
					memset(degrees + capacity, 0,
							sizeof(node_and_degree) * (c - capacity));
					for (unsigned i = capacity; i < c; i++)
						degrees[i].node = i;
					capacity = c;
				}
				
				if (direction == LL_L_UNDIRECTED_ORDERED) {
					if (x.tail > x.head) {
						unsigned t = x.tail; x.tail = x.head; x.head = t;
					}
				}

				degrees[x.tail].degree++;
				if (direction == LL_L_UNDIRECTED_DOUBLE) {
					degrees[x.head].degree++;
				}

				degrees[x.tail].exists = true;
				degrees[x.head].exists = true;

				progress_update(l);
			}
			progress_done(l);

		}
		else /* if (deduplicate) */ {

			ll_external_sort<xs1, xs1_edge_comparator>
				external_sort(&loader_config);

			if (g_progress)
				fprintf(stderr, "Computing node degrees - phase 1:\n  ");

			while (next(f_in, &x)) {
				l++;

				if (x.head >= n) n = x.head;
				if (x.tail >= n) n = x.tail;
				if (n >= capacity) {
					size_t c = capacity;
					while (c <= n + 64) c *= 2;
					degrees = (node_and_degree*)
						realloc(degrees, sizeof(node_and_degree) * c);
					memset(degrees + capacity, 0,
							sizeof(node_and_degree) * (c - capacity));
					for (unsigned i = capacity; i < c; i++)
						degrees[i].node = i;
					capacity = c;
				}

				degrees[x.tail].exists = true;
				degrees[x.head].exists = true;
						
				if (direction == LL_L_UNDIRECTED_ORDERED) {
					if (x.tail > x.head) {
						unsigned t = x.tail; x.tail = x.head; x.head = t;
					}
				}

				external_sort << x;

				if (direction == LL_L_UNDIRECTED_DOUBLE) {
					unsigned t = x.tail; x.tail = x.head; x.head = t;
					external_sort << x;
				}

				progress_update(l);
			}
			progress_done(l);

			external_sort.sort();

			if (g_progress)
				fprintf(stderr, "Computing node degrees - phase 2:\n  ");

			xs1* buffer;
			size_t length;

			unsigned last_tail = (unsigned) -1;
			unsigned last_head = (unsigned) -1;

			l = 0;
			while (external_sort.next_block(&buffer, &length)) {
				while (length --> 0) {

					if (deduplicate && last_head == buffer->head
							&& last_tail == buffer->tail) {
						buffer++;
						continue;
					}

					last_head = buffer->head;
					last_tail = buffer->tail;

					degrees[buffer->tail].degree++;

					buffer++;
					l++;

					progress_update(l);
				}
			}
			progress_done(l);

		} /* if (deduplicate) */

		
		// Degree sort

		size_t num_nodes = n + 1;

		if (degree_order) {
			// TODO Use std::sort
			qsort(degrees, num_nodes, sizeof(node_and_degree), cmp_by_degree_asc);
		}

		node_map = (unsigned*) calloc(num_nodes, sizeof(unsigned));
		unsigned index = 0;
		for (size_t i = 0; i < num_nodes; i++) {
			if (degrees[i].exists)
				node_map[degrees[i].node] = index++;
			else
				node_map[degrees[i].node] = (unsigned) -1;
		}

		free(degrees);
		rewind(f_in);
	}


	// Load the edges into an external sort

	if (g_progress) fprintf(stderr, "Loading the edges:\n  ");

	ll_external_sort<xs1, xs1_edge_comparator> external_sort(&loader_config);

	l = 0;
	while (next(f_in, &x)) {
		l++;

		if (node_map != NULL) {
			x.head = node_map[x.head];
			x.tail = node_map[x.tail];
		}

		if (direction == LL_L_UNDIRECTED_ORDERED) {
			if (x.tail > x.head) {
				unsigned t = x.tail; x.tail = x.head; x.head = t;
			}
		}

		external_sort << x;

		if (direction == LL_L_UNDIRECTED_DOUBLE) {
			unsigned t = x.tail; x.tail = x.head; x.head = t;
			external_sort << x;
		}

		progress_update(l);
	}
	progress_done(l);


	// Save

	external_sort.sort();

	if (g_progress) fprintf(stderr, "Saving the edges:\n  ");
	
	string s_out = dst;
	FILE* f_out = fopen(s_out.c_str(), "wb");
	if (f_out == NULL) {
		perror("fopen");
		return 1;
	}

	xs1* buffer;
	size_t length;

	unsigned last_tail = (unsigned) -1;
	unsigned last_head = (unsigned) -1;

	l = 0;
	while (external_sort.next_block(&buffer, &length)) {
		while (length --> 0) {

			if (deduplicate && last_head == buffer->head
					&& last_tail == buffer->tail) {
				buffer++;
				continue;
			}

			last_head = buffer->head;
			last_tail = buffer->tail;

			ssize_t r = fwrite(buffer, sizeof(xs1), 1, f_out);
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

			progress_update(l);
		}
	}
	progress_done(l);


	// Finalize

	if (node_map) free(node_map);
	if (f_out) fclose(f_out);
	if (f_in) fclose(f_in);

	return 0;
}

