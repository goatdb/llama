/*
 * benchmark.h
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


#ifndef BENCHMARK_H_
#define BENCHMARK_H_

#include <cmath>
#include <cstdio>
#include <unistd.h>

#define LL_B_PROGRESS_LENGTH		40
#define LL_B_PROGRESS_BARSTR		"========================================"
#define LL_B_PROGRESS_NOBARSTR		"                                        "
#define LL_B_PROGRESS_B_Q			"\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b"
#define LL_B_PROGRESS_B_H			LL_B_PROGRESS_B_Q LL_B_PROGRESS_B_Q
#define LL_B_PROGRESS_BACKSPACE 	LL_B_PROGRESS_B_H LL_B_PROGRESS_B_H


/**
 * A benchmark
 */
template <class Graph>
class ll_benchmark {

	std::string _name;


protected:

	Graph& _graph;
	bool _print_progress;


private:

	bool _is_tty;
	size_t _progress_max;
	char _progress_max_str[32];
	int _progress_max_sl;
	int _progress_last_count;


public:

	/**
	 * Create the benchmark
	 *
	 * @param graph the graph
	 * @param name the benchmark name
	 */
	ll_benchmark(Graph& graph, const char* name) : _graph(graph) {

		_name = name;

		_print_progress = false;
		_progress_max = 0;
		_progress_max_sl = 0;
		*_progress_max_str = '\0';

		_is_tty = isatty(fileno(stderr));
	}


	/**
	 * Destroy the benchmark
	 */
	virtual ~ll_benchmark(void) {}


	/**
	 * Get the benchmark name
	 *
	 * @return the name
	 */
	inline const char* name(void) const { return _name.c_str(); }


	/**
	 * Configure whether to print progress
	 *
	 * @param b true to print
	 */
	inline void set_print_progress(bool b) { _print_progress = b; }


	/**
	 * Initialize the benchmark
	 */
	virtual void initialize(void) {}


	/**
	 * Run the benchmark
	 *
	 * @return the numerical result, if applicable
	 */
	virtual double run(void) = 0;


	/**
	 * Finalize the benchmark
	 *
	 * @return the updated numerical result, if applicable
	 */
	virtual double finalize(void) { return NAN; }


	/**
	 * Print the results
	 * 
	 * @param f the output file
	 */
	virtual void print_results(FILE* f) {}


protected:

	/**
	 * Initialize the progress-bar
	 *
	 * @param max the maximum value
	 */
	void progress_init(size_t max) {

		if (!_print_progress) return;
		
		_progress_max = max;
		sprintf(_progress_max_str, "%lu", max);
		_progress_max_sl = (int) strlen(_progress_max_str);
		_progress_last_count = 0;

		if (_is_tty) {
			fprintf(stderr, "[%*s] %*lu/%s ",
					LL_B_PROGRESS_LENGTH, LL_B_PROGRESS_NOBARSTR,
					_progress_max_sl, 0ul, _progress_max_str);
		}
	}


	/**
	 * Print progress if configured to do so
	 *
	 * @param v the value
	 */
	void progress_update(size_t v) {

		if (!_print_progress) return;

		int count = (int) std::min((size_t) LL_B_PROGRESS_LENGTH,
				(LL_B_PROGRESS_LENGTH * v) / _progress_max);

		if (_is_tty) {
			fprintf(stderr, "%.*s[%.*s%.*s] %*lu/%s ",
					LL_B_PROGRESS_LENGTH + 5 + 2 * _progress_max_sl,
					LL_B_PROGRESS_BACKSPACE,
					count, LL_B_PROGRESS_BARSTR,
					LL_B_PROGRESS_LENGTH - count, LL_B_PROGRESS_NOBARSTR,
					_progress_max_sl, v, _progress_max_str);
		}
		else {
			if (_progress_last_count < count) {
				while (_progress_last_count++ < count) {
					fprintf(stderr, ".");
					if (_progress_last_count % (LL_B_PROGRESS_LENGTH/10) == 0) {
						fprintf(stderr, "%d%%", 10 * _progress_last_count
								/ (LL_B_PROGRESS_LENGTH/10));
					}
				}
			}
		}
	}


	/**
	 * Clear the progress-bar
	 *
	 * @param v the value
	 */
	void progress_clear() {

		if (!_print_progress) return;

		if (_is_tty) {
			fprintf(stderr, "%.*s%.*s%.*s",
					LL_B_PROGRESS_LENGTH + 5 + 2*_progress_max_sl,
					LL_B_PROGRESS_BACKSPACE,
					LL_B_PROGRESS_LENGTH + 5 + 2*_progress_max_sl,
					LL_B_PROGRESS_NOBARSTR LL_B_PROGRESS_NOBARSTR,
					LL_B_PROGRESS_LENGTH + 5 + 2*_progress_max_sl,
					LL_B_PROGRESS_BACKSPACE);
		}
		else {
			fprintf(stderr, " ");
		}
	}
};


/**
 * Print part of the results
 *
 * @param f the output file
 * @param graph the graph
 * @param a the array of results
 * @param max the max number of results to print
 */
template <class Graph>
void print_results_part(FILE* f, Graph& graph, int* a, int max=50) {

	node_t m = std::min<node_t>(max, graph.max_nodes());

	for (node_t n = 0; n < m; n++) {
		if (n % 10 == 0) fprintf(f, "%7ld:", n);
		fprintf(f, " %7d", a[n]);
		if (n % 10 == 9 || n + 1 == m) fprintf(f, "\n");
	}

	fprintf(f, "\n");
}


/**
 * Print part of the results
 *
 * @param f the output file
 * @param graph the graph
 * @param a the array of results
 * @param max the max number of results to print
 */
template <class Graph>
void print_results_part(FILE* f, Graph& graph, long* a, int max=50) {

	node_t m = std::min<node_t>(max, graph.max_nodes());

	for (node_t n = 0; n < m; n++) {
		if (n % 10 == 0) fprintf(f, "%7ld:", n);
		fprintf(f, " %7ld", a[n]);
		if (n % 10 == 9 || n + 1 == m) fprintf(f, "\n");
	}

	fprintf(f, "\n");
}


/**
 * Print part of the results
 *
 * @param f the output file
 * @param graph the graph
 * @param a the array of results
 * @param max the max number of results to print
 */
template <class Graph>
void print_results_part(FILE* f, Graph& graph, float* a, int max=50) {

	node_t m = std::min<node_t>(max, graph.max_nodes());

	for (node_t n = 0; n < m; n++) {
		if (n % 10 == 0) fprintf(f, "%7ld:", n);
		fprintf(f, " %0.10f", a[n]);
		if (n % 10 == 9 || n + 1 == m) fprintf(f, "\n");
	}

	fprintf(f, "\n");
}


/**
 * Print part of the results
 *
 * @param f the output file
 * @param graph the graph
 * @param a the array of results
 * @param max the max number of results to print
 */
template <class Graph>
void print_results_part(FILE* f, Graph& graph, double* a, int max=50) {

	node_t m = std::min<node_t>(max, graph.max_nodes());

	for (node_t n = 0; n < m; n++) {
		if (n % 10 == 0) fprintf(f, "%7ld:", n);
		fprintf(f, " %0.10lf", a[n]);
		if (n % 10 == 9 || n + 1 == m) fprintf(f, "\n");
	}

	fprintf(f, "\n");
}


#endif
