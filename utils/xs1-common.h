/*
 * xs1-common.h
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


#ifndef XS1_COMMON_H_
#define XS1_COMMON_H_

#include <cstdio>
#include <cstdlib>
#include <libgen.h>


/*
 * Debug output
 */

#ifdef _DEBUG
#define LL_D_PRINT(format, ...) { \
	fprintf(stderr, "[DEBUG] %s" format, __FUNCTION__, ## __VA_ARGS__); }
#else
#define LL_D_PRINT(format, ...)
#endif


/*
 * Error handling
 */

#define LL_W_PRINT(format, ...) { \
	fprintf(stderr, "[WARN ] %s" format, __FUNCTION__, ## __VA_ARGS__); }
#define LL_E_PRINT(format, ...) { \
	fprintf(stderr, "[ERROR] %s" format, __FUNCTION__, ## __VA_ARGS__); }


/**
 * The X-Stream Type 1 entry
 */
struct xs1 {
	unsigned tail;
	unsigned head;
	float weight;
};


/**
 * Read the next Type 1 entry
 *
 * @param f the file
 * @param out the output
 * @return true if anything was read
 */
bool next(FILE* f, xs1* out) {
	ssize_t r = fread(out, sizeof(xs1), 1, f);
	if (r < 0) {
		perror("fread");
		abort();
	}
	return r > 0;
}


/**
 * Write the ini file
 *
 * @param dat_name the original (.dat) file name
 * @param num_nodes the number of nodes
 * @param num_edges the number of edges
 * @return true if okay, false if not
 */
bool write_ini(const char* dat_name, size_t num_nodes, size_t num_edges) {

	std::string s_out = dat_name; s_out += ".ini";
	FILE* f_out = fopen(s_out.c_str(), "w");
	if (f_out == NULL) return false;

	char* name = (char*) alloca(strlen(dat_name) + 1);
	memcpy(name, dat_name, strlen(dat_name) + 1);
	name = basename(name);

	fprintf(f_out, "[graph]\n");
	fprintf(f_out, "type=1\n");
	fprintf(f_out, "name=%s\n", name);
	fprintf(f_out, "vertices=%lu\n", num_nodes);
	fprintf(f_out, "edges=%lu\n", num_edges);

	fclose(f_out);

	return true;
}


#endif
