/*
 * xs1-max-degree.cpp
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
 * Find the node(s) with the highest degrees
 *
 * DISCLAIMER: This tool was developed separately from X-Stream and without
 * their endorsement.
 */

#include <sys/types.h>
#include <sys/stat.h>
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

	int max = 10;
	bool progress = true;
	unsigned progress_step = 10000000;

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

	size_t capacity = 80 * 1000 * 1000;
	node_and_degree* degrees
		= (node_and_degree*) malloc(sizeof(node_and_degree) * capacity);
	memset(degrees, 0, sizeof(node_and_degree) * capacity);
	for (unsigned i = 0; i < capacity; i++) degrees[i].node = i;
	
	xs1 x;
	unsigned n = 0;
	unsigned l = 0;

	while (next(f_in, &x)) {
		l++;
		
		if (x.head >= n) n = x.head;
		if (x.tail >= n) n = x.tail;
		if (n >= capacity) {
			size_t c = capacity;
			while (c <= n + 64) c *= 2;
			degrees = (node_and_degree*) realloc(degrees, sizeof(node_and_degree) * c);
			memset(degrees + capacity, 0, sizeof(node_and_degree) * (c - capacity));
			for (unsigned i = capacity; i < c; i++) degrees[i].node = i;
			capacity = c;
		}

		degrees[x.tail].degree++;

		if (progress && (l % progress_step) == 0) {
			fprintf(stderr, ".");
			if ((l % (progress_step * 10)) == 0) {
				fprintf(stderr, "%um", l / 1000000);
			}
			if ((l % (progress_step * 50)) == 0) {
				fprintf(stderr, "\n");
			}
		}
	}
	if (progress) {
		if ((l % (progress_step * 50)) >= progress_step) fprintf(stderr, "\n");
	}

	fclose(f_in);

	n++;
	qsort(degrees, n, sizeof(node_and_degree), cmp_by_degree_desc);

	if ((unsigned) max > n) max = (int) n;
	printf("Node\tDegree\tRank\n");
	for (int i = 0; i < max; i++) {
		printf("%u\t%d\t%d\n", degrees[i].node, degrees[i].degree, i+1);
	}

	return 0;
}

