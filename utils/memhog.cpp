/*
 * memhog.cpp
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


/**
 * Decrease the effective memory size by mlock-ing memory
 */

#include <sys/mman.h>
#include <sys/types.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


/**
 * The main function
 */
int main(int argc, char** argv) {

	if (argc != 2) {
		fprintf(stderr, "Usage: %s MEM_GB\n", argv[0]);
		return 1;
	}

	int gb = atoi(argv[1]);

	char buf[64];
	sprintf(buf, "%d", gb);
	if (strcmp(buf, argv[1]) != 0) {
		fprintf(stderr, "Error: Cannot parse the memory size argument\n");
		return 1;
	}
	if (gb <= 0) {
		fprintf(stderr, "Error: Must specify at least 1 GB to hog\n");
		return 1;
	}

	size_t size = ((size_t) gb) * 1024 * 1024 * 1024;
	void* m = malloc(size);
	if (m == NULL) {
		fprintf(stderr, "Error: Not enough memory\n");
		return 1;
	}

	if (mlock(m, size)) {
		if (errno == ENOMEM) {
			fprintf(stderr, "Error: Could not mlock -- insufficient permissions\n");
		}
		else {
			fprintf(stderr, "Error: Could not mlock -- %s\n", strerror(errno));
		}
		free(m);
		return 1;
	}

	memset(m, 0xcc, size);		// Is this necessary?

	fprintf(stderr, "Hogging %d GB RAM. Use Ctrl+C to exit.\n", gb);
	while (1) sleep(60);

	return 0;
}

