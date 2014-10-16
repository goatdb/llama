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
#include <vector>

#include <unistd.h>

#define LL_B_PROGRESS_LENGTH		40
#define LL_B_PROGRESS_BARSTR		"========================================"
#define LL_B_PROGRESS_NOBARSTR		"                                        "
#define LL_B_PROGRESS_B_Q			"\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b"
#define LL_B_PROGRESS_B_H			LL_B_PROGRESS_B_Q LL_B_PROGRESS_B_Q
#define LL_B_PROGRESS_BACKSPACE 	LL_B_PROGRESS_B_H LL_B_PROGRESS_B_H


/**
 * Various length correspondence options
 */
#define LL_BA_LC_NONE				0
#define LL_BA_LC_NODES				1


/**
 * An array used in a benchmark
 */
typedef struct {

	/// The array
	void* ba_data;

	/// Variable
	void** ba_variable;

	/// Element size
	size_t ba_element_size;

	/// Length
	size_t ba_length;

	/// Lenght correspondence
	int ba_length_correspondence;

} ll_benchmark_array_t;


/**
 * An edge property used in a benchmark
 */
typedef struct {

	/// The property variable
	void** bp_variable;

	/// The property name
	std::string bp_name;

	/// Element size
	size_t bp_element_size;

	/// Required?
	bool bp_required;

} ll_benchmark_edge_property_t;


/**
 * A node property used in a benchmark
 */
typedef struct {

	/// The property variable
	void** bp_variable;

	/// The property name
	std::string bp_name;

	/// Element size
	size_t bp_element_size;

	/// Required?
	bool bp_required;

} ll_benchmark_node_property_t;


/**
 * A benchmark
 */
template <class Graph>
class ll_benchmark {

	std::string _name;


protected:

	Graph* _graph;
	bool _print_progress;


private:

	bool _is_tty;
	size_t _progress_max;
	char _progress_max_str[32];
	int _progress_max_sl;
	int _progress_last_count;

	std::vector<ll_benchmark_array_t> _auto_arrays;
	std::vector<ll_benchmark_edge_property_t> _auto_edge_properties;
	std::vector<ll_benchmark_node_property_t> _auto_node_properties;


public:

	/**
	 * Create the benchmark
	 *
	 * @param name the benchmark name
	 */
	ll_benchmark(const char* name) {

		_name = name;
		_graph = NULL;

		_print_progress = false;
		_progress_max = 0;
		_progress_max_sl = 0;
		*_progress_max_str = '\0';

		_is_tty = isatty(fileno(stderr));
	}


	/**
	 * Destroy the benchmark
	 */
	virtual ~ll_benchmark(void) {

		for (size_t i = 0; i < _auto_arrays.size(); i++) {
			if (_auto_arrays[i].ba_data != NULL) {
				free(_auto_arrays[i].ba_data);
				*_auto_arrays[i].ba_variable = NULL;
			}
		}
	}


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
	 * Get the graph
	 *
	 * @return the graph
	 */
	inline Graph* graph(void) { return _graph; }


	/**
	 * Initialize the benchmark and set the graph
	 *
	 * @param graph the graph
	 */
	virtual void initialize(Graph* graph) {

		_graph = graph;
		if (_graph == NULL) return;


		// Resize the auto arrays

		for (size_t i = 0; i < _auto_arrays.size(); i++) {
			switch (_auto_arrays[i].ba_length_correspondence) {
				case LL_BA_LC_NODES:
					if ((ssize_t) _graph->max_nodes()
							> (ssize_t) _auto_arrays[i].ba_length) {

						if (_auto_arrays[i].ba_data != NULL) {
							free(_auto_arrays[i].ba_data);
						}

						_auto_arrays[i].ba_length = _graph->max_nodes();
						_auto_arrays[i].ba_data = malloc
							(_auto_arrays[i].ba_element_size
								* (_auto_arrays[i].ba_length + 16));
						*_auto_arrays[i].ba_variable = _auto_arrays[i].ba_data;
					}
					break;
			}
		}


		// Initialize the properties

		for (size_t i = 0; i < _auto_edge_properties.size(); i++) {
			switch (_auto_edge_properties[i].bp_element_size) {
				case sizeof(uint32_t):
					*_auto_edge_properties[i].bp_variable
					 = _graph->get_edge_property_32(
							 _auto_edge_properties[i].bp_name.c_str());
					break;
				case sizeof(uint64_t):
					*_auto_edge_properties[i].bp_variable
					 = _graph->get_edge_property_64(
							 _auto_edge_properties[i].bp_name.c_str());
					break;
				default:
					abort();
			}

			if (*_auto_edge_properties[i].bp_variable == NULL
					&& _auto_edge_properties[i].bp_required) {
				LL_E_PRINT("Error: The graph does not have edge property "
						"\"%s\".\n", _auto_edge_properties[i].bp_name.c_str());
				abort();
			}
		}

		for (size_t i = 0; i < _auto_node_properties.size(); i++) {
			switch (_auto_node_properties[i].bp_element_size) {
				case sizeof(uint32_t):
					*_auto_node_properties[i].bp_variable
					 = _graph->get_node_property_32(
							 _auto_node_properties[i].bp_name.c_str());
					break;
				case sizeof(uint64_t):
					*_auto_node_properties[i].bp_variable
					 = _graph->get_node_property_64(
							 _auto_node_properties[i].bp_name.c_str());
					break;
				default:
					abort();
			}

			if (*_auto_node_properties[i].bp_variable == NULL
					&& _auto_node_properties[i].bp_required) {
				LL_E_PRINT("Error: The graph does not have node property "
						"\"%s\".\n", _auto_node_properties[i].bp_name.c_str());
				abort();
			}
		}
	}


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
	 * Create a new array that will be automatically resized and deallocated
	 * on exit that is parallel to the vertices
	 *
	 * @param variable the variable
	 */
	template <typename T>
	void create_auto_array_for_nodes(T*& variable) {

		// Static assertions

		int _s1[sizeof(T* ) == sizeof(void* ) ? 1 : -1]; (void) _s1;
		int _s2[sizeof(T**) == sizeof(void**) ? 1 : -1]; (void) _s2;


		// Create and register the auto array

		ll_benchmark_array_t b;

		b.ba_variable = (void**) &variable;
		b.ba_element_size = sizeof(T);
		b.ba_length_correspondence = LL_BA_LC_NODES;

		if (_graph != NULL) {
			b.ba_length = _graph->max_nodes();
			b.ba_data = malloc(b.ba_element_size
					* (b.ba_length + 16));
		}
		else {
			b.ba_length = 0;
			b.ba_data = NULL;
		}

		variable = (T*) b.ba_data;

		_auto_arrays.push_back(b);
	}


	/**
	 * Register an automatic edge property variable
	 *
	 * @param variable the variable
	 * @param name the property name
	 * @param required true if the property is required
	 */
	template <typename T>
	void create_auto_property(ll_mlcsr_edge_property<T>*& variable,
			const char* name, bool required=true) {

		// Static assertions

		int _s1[sizeof(ll_mlcsr_edge_property<T>* ) == sizeof(void* ) ? 1 : -1];
		int _s2[sizeof(ll_mlcsr_edge_property<T>**) == sizeof(void**) ? 1 : -1];
		int _s3[(sizeof(T) == sizeof(uint32_t)
				|| sizeof(T) == sizeof(uint64_t)) ? 1 : -1];
		(void) _s1; (void) _s2; (void) _s3;


		// Create and register the auto property

		ll_benchmark_edge_property_t b;

		b.bp_variable = (void**) &variable;
		b.bp_name = name;
		b.bp_required = required;
		b.bp_element_size = sizeof(T);

		if (_graph != NULL) {
			switch (b.bp_element_size) {
				case sizeof(uint32_t):
					variable = reinterpret_cast<ll_mlcsr_edge_property<T>*>(
							_graph->get_edge_property_32(b.bp_name.c_str()));
					break;
				case sizeof(uint64_t):
					variable = reinterpret_cast<ll_mlcsr_edge_property<T>*>(
							_graph->get_edge_property_64(b.bp_name.c_str()));
					break;
				default:
					abort();
			}

			if (variable == NULL && b.bp_required) {
				LL_E_PRINT("Error: The graph does not have edge property "
						"\"%s\".\n", b.bp_name.c_str());
				abort();
			}
		}
		else {
			variable = NULL;
		}

		_auto_edge_properties.push_back(b);
	}


	/**
	 * Register an automatic node property variable
	 *
	 * @param variable the variable
	 * @param name the property name
	 * @param required true if the property is required
	 */
	template <typename T>
	void create_auto_property(ll_mlcsr_node_property<T>*& variable,
			const char* name, bool required=true) {

		// Static assertions

		int _s1[sizeof(ll_mlcsr_node_property<T>* ) == sizeof(void* ) ? 1 : -1];
		int _s2[sizeof(ll_mlcsr_node_property<T>**) == sizeof(void**) ? 1 : -1];
		int _s3[(sizeof(T) == sizeof(uint32_t)
				|| sizeof(T) == sizeof(uint64_t)) ? 1 : -1];
		(void) _s1; (void) _s2; (void) _s3;


		// Create and register the auto property

		ll_benchmark_node_property_t b;

		b.bp_variable = (void**) &variable;
		b.bp_name = name;
		b.bp_required = required;
		b.bp_element_size = sizeof(T);

		if (_graph != NULL) {
			switch (b.bp_element_size) {
				case sizeof(uint32_t):
					variable = reinterpret_cast<ll_mlcsr_node_property<T>*>(
							_graph->get_node_property_32(b.bp_name.c_str()));
					break;
				case sizeof(uint64_t):
					variable = reinterpret_cast<ll_mlcsr_node_property<T>*>(
							_graph->get_node_property_64(b.bp_name.c_str()));
					break;
				default:
					abort();
			}

			if (variable == NULL && b.bp_required) {
				LL_E_PRINT("Error: The graph does not have node property "
						"\"%s\".\n", b.bp_name.c_str());
				abort();
			}
		}
		else {
			variable = NULL;
		}

		_auto_node_properties.push_back(b);
	}


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
void print_results_part(FILE* f, Graph* graph, int* a, int max=50) {

	node_t m = std::min<node_t>(max, graph->max_nodes());

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
void print_results_part(FILE* f, Graph* graph, long* a, int max=50) {

	node_t m = std::min<node_t>(max, graph->max_nodes());

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
void print_results_part(FILE* f, Graph* graph, float* a, int max=50) {

	node_t m = std::min<node_t>(max, graph->max_nodes());

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
void print_results_part(FILE* f, Graph* graph, double* a, int max=50) {

	node_t m = std::min<node_t>(max, graph->max_nodes());

	for (node_t n = 0; n < m; n++) {
		if (n % 10 == 0) fprintf(f, "%7ld:", n);
		fprintf(f, " %0.10lf", a[n]);
		if (n % 10 == 9 || n + 1 == m) fprintf(f, "\n");
	}

	fprintf(f, "\n");
}


#endif
