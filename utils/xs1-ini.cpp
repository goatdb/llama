/*
 * xs1-ini.cpp
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
 * Create an ini file
 *
 * DISCLAIMER: This tool was developed separately from X-Stream and without
 * their endorsement.
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <libgen.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unistd.h>
#include "xs1-common.h"

using namespace std;


struct node_and_degree {
	unsigned node;
	int degree;
};


int cmp_by_degree_desc(const void* a, const void* b) {
	const node_and_degree* A = (const node_and_degree*) a;
	const node_and_degree* B = (const node_and_degree*) b;
	return B->degree - A->degree;
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
	size_t progress_step = 10000000;

	if (argc != 2) {
		fprintf(stderr, "Usage: %s FILE\n", argv[0]);
		return 1;
	}
	
	string s_in = argv[1];
	FILE* f_in = fopen(s_in.c_str(), "rb");
	if (f_in == NULL) {
		perror("fopen");
		return 1;
	}
	
	string s_out = argv[1]; s_out += ".ini";
	FILE* f_out_test = fopen(s_out.c_str(), "r");
	if (f_out_test != NULL) {
		fprintf(stderr, "Error: File %s exists\n", s_out.c_str());
		fclose(f_out_test);
		fclose(f_in);
		return 1;
	}
	
	xs1 x;
	size_t num_nodes = 0;
	size_t num_edges = 0;

	while (next(f_in, &x)) {

		num_edges++;

		if (x.head >= num_nodes) num_nodes = x.head + 1;
		if (x.tail >= num_nodes) num_nodes = x.tail + 1;

		if (progress && (num_edges % progress_step) == 0) {
			fprintf(stderr, ".");
			if ((num_edges % (progress_step * 10)) == 0) {
				fprintf(stderr, "%lum", num_edges / 1000000);
			}
			if ((num_edges % (progress_step * 50)) == 0) {
				fprintf(stderr, "\n");
			}
		}
	}

	if (progress) {
		if ((num_edges % (progress_step * 50)) >= progress_step) {
			fprintf(stderr, "\n");
		}
	}

	fclose(f_in);

	if (!write_ini(s_in.c_str(), num_nodes, num_edges)) {
		perror("write_ini");
		return 1;
	}

	return 0;
}

