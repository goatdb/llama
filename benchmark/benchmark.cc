/*
 * benchmark.cc
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
#include <omp.h>
#include <getopt.h>
#include <libgen.h>

#include <algorithm>
#include <iomanip>
#include <sstream>
#include <string>

#include <llama.h>

#include "benchmarks/avg_teen_cnt.h"
#include "benchmarks/bc_adj.h"
#include "benchmarks/bc_random.h"
#include "benchmarks/bfs.h"
#include "benchmarks/pagerank.h"
#include "benchmarks/sssp.h"
#include "benchmarks/tarjan_scc.h"
#include "benchmarks/triangle_counting.h"

#include "tests/delete_edges.h"
#include "tests/delete_nodes.h"

#include "tools/cross_validate.h"
#include "tools/level_spread.h"
#include "tools/degree_distribution.h"
#include "tools/flatten.h"
#include "tools/dump.h"
#include "tools/property_stats.h"


/*
 * Customize the benchmark using the following preprocessor directives:
 *
 * BENCHMARK_WRITABLE
 * BENCHMARK_TASK_ID
 * BENCHMARK_CONCURRENT_LOAD
 * DO_CP
 * TEST_CXX_ITER
 */



//==========================================================================//
// Global Initialization & Sanity Checks                                    //
//==========================================================================//

//#define BENCHMARK_WRITABLE
//#define BENCHMARK_CONCURRENT_LOAD
//#define DO_CP
#define TEST_CXX_ITER

#ifdef BENCHMARK_WRITABLE
typedef ll_writable_graph benchmarkable_graph_t;
#define benchmarkable_graph(g)	(g)
#else
typedef ll_mlcsr_ro_graph benchmarkable_graph_t;
#define benchmarkable_graph(g)	((g).ro_graph())
#endif

#ifdef BENCHMARK_WRITABLE
#	ifndef LL_DELETIONS
#		warning "Attempting to use BENCHMARK_WRITABLE without LL_DELETIONS"
#	endif
#	ifdef LL_STREAMING
#		error "Using BENCHMARK_WRITABLE with LL_STREAMING"
#	endif
#endif



//==========================================================================//
// Testing                                                                  //
//==========================================================================//


template <class EXP_GRAPH>
node_t __attribute__ ((noinline)) TEST_FUNCTION(EXP_GRAPH& G_exp) {

	node_t u = 0x3;
	node_t nothing = 0;

	if (u >= G_exp.max_nodes()) return nothing;

	ll_edge_iterator iter;
	G_exp.out_iter_begin(iter, u);
	for (edge_t v_idx = G_exp.out_iter_next(iter);
			v_idx != LL_NIL_EDGE;
			v_idx = G_exp.out_iter_next(iter)) {
		nothing ^= LL_ITER_OUT_NEXT_NODE(G_exp, iter, v_idx);
	}

	/*typename EXP_GRAPH::iterator iter;
	for (iter = G_exp.begin(u); iter != G_exp.end(); ++iter) {
		nothing ^= *iter;
	}*/

	return nothing;
}


//==========================================================================//
// The Tested Simple OLTP Functions                                         //
//==========================================================================//

template <class EXP_W_GRAPH>
void w_test(EXP_W_GRAPH& G)
{
	printf("\n\nTEST START\n");

	G.tx_begin();

	edge_t e1 = G.add_edge(0, 1);
	edge_t e2 = G.add_edge(1, 2);

	printf("%lld --> %lld\n", G.src(e1), G.dst(e1));
	printf("%lld --> %lld\n", G.src(e2), G.dst(e2));

	node_t n = 0;
	printf("TEST%3u:", (unsigned) n);
	ll_edge_iterator iter = G.out(n);
	for (edge_t v_idx = G.iter_next(iter); v_idx != LL_NIL_EDGE;
			v_idx = G.iter_next(iter)) {
		node_t v = LL_ITER_OUT_NEXT_NODE(G, iter, v_idx);
		printf(" %7u", (unsigned) v);
	}
	printf("\n");

	G.tx_commit();

	printf("DID NOT CRASH :)\n");
}



//==========================================================================//
// Supporting Functions                                                     //
//==========================================================================//


/**
 * Print time in appropriate units
 *
 * @param f the output file
 * @param prefix the prefix
 * @param t the time in ms
 * @param newline true to print the newline
 */
static void print_time(FILE* f, const char* prefix, double t, bool newline=true) {
	fprintf(f, "%s%0.3lf seconds", prefix, t/1000.0);
	if (t > 45000) fprintf(f, " (%0.2lf min)", t/60000.0);
	if (newline) fprintf(f, "\n");
}


/**
 * Print time and confidence in appropriate units
 *
 * @param f the output file
 * @param prefix the prefix
 * @param t the time in ms
 * @param c the confidence interval size
 * @param newline true to print the newline
 */
static void print_time_and_confidence(FILE* f, const char* prefix, double t,
		double c, bool newline=true) {
	fprintf(f, "%s%0.3lf +- %0.3lf seconds", prefix, t/1000.0, c/1000.0);
	if (t > 45000) fprintf(f, " (%0.2lf +- %0.2lf min)", t/60000.0, c/60000.0);
	if (newline) fprintf(f, "\n");
}


#if defined(__linux__)

/**
 * The I/O info
 */
struct io_stat {
	size_t io_rchar;
	size_t io_wchar;
	size_t io_syscr;
	size_t io_syscw;
	size_t io_read_bytes;
	size_t io_write_bytes;
	size_t io_cancelled_write_bytes;
};


/**
 * Get the I/O info about the process
 *
 * @param io the pointer to the io_stat struct
 * @return 0 if okay, -1 on error
 */
int getiostat(struct io_stat* io) {
	memset(io, 0, sizeof(io_stat));

	FILE* f = fopen("/proc/self/io", "r");
	if (!f) return -1;

	char* line = NULL;
	size_t len = 0;
	ssize_t read;

	while ((read = getline(&line, &len, f)) != -1) {

		char* p = strchr(line, ':');
		if (p == NULL) continue;
		*(p++) = '\0';
		while (*p != '\0' && isspace(*p)) p++;

		if (strcmp(line, "rchar") == 0) io->io_rchar = atol(p);
		if (strcmp(line, "wchar") == 0) io->io_wchar = atol(p);
		if (strcmp(line, "syscr") == 0) io->io_syscr = atol(p);
		if (strcmp(line, "syscw") == 0) io->io_syscw = atol(p);
		if (strcmp(line, "read_bytes") == 0) io->io_read_bytes = atol(p);
		if (strcmp(line, "write_bytes") == 0) io->io_write_bytes = atol(p);
		if (strcmp(line, "cancelled_write_bytes") == 0)
			io->io_cancelled_write_bytes = atol(p);
	}

	if (line) free(line);
	fclose(f);

	return 0;
}

#endif


/**
 * Statistics, counters, and the corresponding temporary contexts
 */
class ll_benchmark_stats {

public:

	time_t start_time;
	struct tm tm_start_time;

	long maxrss_start;
	long maxrss_loaded;

	std::vector<double> load_times;
	std::vector<double> load_pull_times;
	std::vector<double> load_cp_times;

	std::vector<double> delete_snapshot_times;
	
	std::vector<double> runtimes;
	std::vector<double> cpu_times;
	std::vector<double> cpu_user_times;
	std::vector<double> cpu_sys_times;
	std::vector<double> cpu_util;

	std::vector<long> major_faults;
	std::vector<long> in_blocks;
	std::vector<long> out_blocks;

	std::vector<size_t> io_rchar;
	std::vector<size_t> io_wchar;
	std::vector<size_t> io_syscr;
	std::vector<size_t> io_syscw;
	std::vector<size_t> io_read_bytes;
	std::vector<size_t> io_write_bytes;
	std::vector<size_t> io_cancelled_write_bytes;


private:

	// Benchmark stats
	
#if defined(__linux__)
	struct io_stat _b_io_start;
#endif
	struct rusage _b_ru_start;
	double _b_t_start;


	// Load stats
	
	double _l_t_start;
	double _l_t_pull_start;
	double _l_t_cp_start;


	// Delete stats
	
	double _d_t_start;


public:

	/**
	 * Create an instance of ll_benchmark_stats
	 */
	ll_benchmark_stats(void) {

		struct rusage ru_start;
		getrusage(RUSAGE_SELF, &ru_start);
		maxrss_start = ru_start.ru_maxrss;

		maxrss_loaded = maxrss_start;	// Zero this relative to maxrss_start

		start_time = time(NULL);
		localtime_r(&start_time, &tm_start_time);
	}


	/**
	 * Start counters before benchmark
	 */
	void before_benchmark(void) {

#if defined(__linux__)
		getiostat(&_b_io_start);
#endif

		getrusage(RUSAGE_SELF, &_b_ru_start);
		_b_t_start = ll_get_time_ms();
	}


	/**
	 * Stop counters after benchmark
	 */
	void after_benchmark(void) {

		double t = ll_get_time_ms() - _b_t_start;
		struct rusage ru_end;
		getrusage(RUSAGE_SELF, &ru_end);

#if defined(__linux__)
		struct io_stat io_end;
		getiostat(&io_end);
#endif

		runtimes.push_back(t);
		cpu_times.push_back(ll_timeval_to_ms(ru_end.ru_utime)
				+ ll_timeval_to_ms(ru_end.ru_stime)
				- ll_timeval_to_ms(_b_ru_start.ru_utime)
				- ll_timeval_to_ms(_b_ru_start.ru_stime));
		cpu_user_times.push_back(ll_timeval_to_ms(ru_end.ru_utime)
				- ll_timeval_to_ms(_b_ru_start.ru_utime));
		cpu_sys_times.push_back(ll_timeval_to_ms(ru_end.ru_stime)
				- ll_timeval_to_ms(_b_ru_start.ru_stime));
		cpu_util.push_back((cpu_times[cpu_times.size()-1]
					/ omp_get_max_threads()) / t);
		major_faults.push_back(ru_end.ru_majflt - _b_ru_start.ru_majflt);
		in_blocks.push_back(ru_end.ru_inblock - _b_ru_start.ru_inblock);
		out_blocks.push_back(ru_end.ru_oublock - _b_ru_start.ru_oublock);

#if defined(__linux__)
		io_rchar.push_back(io_end.io_rchar - _b_io_start.io_rchar);
		io_wchar.push_back(io_end.io_wchar - _b_io_start.io_wchar);
		io_syscr.push_back(io_end.io_syscr - _b_io_start.io_syscr);
		io_syscw.push_back(io_end.io_syscw - _b_io_start.io_syscw);
		io_read_bytes.push_back(io_end.io_read_bytes
				- _b_io_start.io_read_bytes);
		io_write_bytes.push_back(io_end.io_write_bytes
				- _b_io_start.io_write_bytes);
		io_cancelled_write_bytes.push_back(
				io_end.io_cancelled_write_bytes
				- _b_io_start.io_cancelled_write_bytes);
#endif
	}


	/**
	 * Start counters before load
	 */
	void before_load(void) {

		_l_t_start = ll_get_time_ms();
	}


	/**
	 * Stop counters after load
	 */
	void after_load(void) {

		double t = ll_get_time_ms() - _l_t_start;
		load_times.push_back(t);

		struct rusage r_loaded;
		getrusage(RUSAGE_SELF, &r_loaded);
		maxrss_loaded = r_loaded.ru_maxrss;
	}


	/**
	 * Start counters before load's pull phase
	 */
	void before_load_pull(void) {

		_l_t_pull_start = ll_get_time_ms();
	}


	/**
	 * Stop counters after load's pull phase
	 */
	void after_load_pull(void) {

		double t = ll_get_time_ms() - _l_t_pull_start;
		load_pull_times.push_back(t);
	}


	/**
	 * Start counters before load's CP phase
	 */
	void before_load_cp(void) {

		_l_t_cp_start = ll_get_time_ms();
	}


	/**
	 * Stop counters after load's CP phase
	 */
	void after_load_cp(void) {

		double t = ll_get_time_ms() - _l_t_cp_start;
		load_cp_times.push_back(t);
	}


	/**
	 * Start counters before deleting a snapshot
	 */
	void before_delete_snapshot(void) {

		_d_t_start = ll_get_time_ms();
	}


	/**
	 * Stop counters after deleting a snapshot
	 */
	void after_delete_snapshot(void) {

		double t = ll_get_time_ms() - _d_t_start;
		delete_snapshot_times.push_back(t);
	}


	/**
	 * Print the statistics
	 *
	 * @param f the output file
	 */
	void print_stats(FILE* f = stdout) {

		if (load_times.size() > 0) {
			fprintf(f, "\nLOAD\n");
#ifdef LL_STREAMING
			print_time(f, "Load Time  : ",
					ll_mean(load_times), false);
			print_time(f, " each, ", ll_sum(load_times), false);
			fprintf(f, " total\n");
#else
			print_time(f, "Load Time  : ", ll_mean(load_times));
#endif
			fprintf(f, "Memory     : %0.2lf MB\n",
					(maxrss_loaded - maxrss_start) / 1024.0);
		}

		if (runtimes.size() > 0) {
			if (runtimes.size() == 1) {

				fprintf(f, "\nTIME\n");
				print_time(f, "Time       : ", ll_sum(runtimes));
				print_time(f, "User Time  : ", ll_sum(cpu_user_times));
				print_time(f, "System Time: ", ll_sum(cpu_sys_times));
				print_time(f, "CPU Time   : ", ll_sum(cpu_times));
				fprintf(f, "CPU Util.  : %0.2lf%%\n", 100*ll_mean(cpu_util));

				fprintf(f, "\nRESOURCE USAGE\n");
				fprintf(f, "Mjr Faults : %ld\n", ll_sum(major_faults));
				fprintf(f, "In Blocks  : %ld\n", ll_sum(in_blocks));
				fprintf(f, "Out Blocks : %ld\n", ll_sum(out_blocks));
#if defined(__linux__)
				fprintf(f, "In Bytes   : %ld (%0.2lf GB)\n",
						ll_sum(io_read_bytes),
						ll_sum(io_read_bytes) / (1024.0*1024.0*1024.0));
				fprintf(f, "Out Bytes  : %ld (%0.2lf GB)\n",
						ll_sum(io_write_bytes),
						ll_sum(io_write_bytes) / (1024.0*1024.0*1024.0));
#endif
			}
			else {
				fprintf(f, "\nTIME\n");
#ifdef LL_STREAMING
				print_time_and_confidence(f, "Time       : ",
						ll_mean(runtimes), ll_c95(runtimes), false);
				print_time(f, " each, ", ll_sum(runtimes), false);
				fprintf(f, " total\n");
#else
				print_time_and_confidence(f, "Time       : ",
						ll_mean(runtimes), ll_c95(runtimes));
#endif
				print_time_and_confidence(f, "User Time  : ",
						ll_mean(cpu_user_times), ll_c95(cpu_user_times));
				print_time_and_confidence(f, "System Time: ",
						ll_mean(cpu_sys_times), ll_c95(cpu_sys_times));
				print_time_and_confidence(f, "CPU Time   : ",
						ll_mean(cpu_times), ll_c95(cpu_times));
				fprintf(f, "CPU Util.  : %0.2lf +- %0.2lf%%\n",
						100*ll_mean(cpu_util), 100*ll_c95(cpu_util));

				fprintf(f, "\nRESOURCE USAGE\n");
				fprintf(f, "Mjr Faults : %0.2lf +- %0.2lf\n",
						ll_mean(major_faults), ll_c95(major_faults));
				fprintf(f, "In Blocks  : %0.2lf +- %0.2lf\n",
						ll_mean(in_blocks), ll_c95(in_blocks));
				fprintf(f, "Out Blocks : %0.2lf +- %0.2lf\n",
						ll_mean(out_blocks), ll_c95(out_blocks));
#if defined(__linux__)
				fprintf(f, "In Bytes   : %0.2lf +- %0.2lf "
						"(%0.2lf +- %0.2lf GB)\n",
						ll_mean(io_read_bytes),
						ll_c95(io_read_bytes),
						ll_mean(io_read_bytes) / (1024.0*1024.0*1024.0),
						ll_c95(io_read_bytes) / (1024.0*1024.0*1024.0));
				fprintf(f, "Out Butes  : %0.2lf +- %0.2lf "
						"(%0.2lf +- %0.2lf GB)\n",
						ll_mean(io_write_bytes),
						ll_c95(io_write_bytes),
						ll_mean(io_write_bytes) / (1024.0*1024.0*1024.0),
						ll_c95(io_write_bytes) / (1024.0*1024.0*1024.0));
#endif
			}
		}
		printf("\n");

		fflush(f);
	}


	/**
	 * Save the statistics to a file
	 *
	 * @param G the graph
	 * @param representation the representation/configuration identifier
	 * @param task the task identifier
	 */
	template <class Graph>
	void save_stats(Graph& G, const char* representation, const char* task) {


		// Generate the file name

		char date_time[64];
		snprintf(date_time, 64, "%04d%02d%02d-%02d%02d%02d",
				tm_start_time.tm_year + 1900, tm_start_time.tm_mon + 1,
				tm_start_time.tm_mday, tm_start_time.tm_hour,
				tm_start_time.tm_min, tm_start_time.tm_sec);

		std::string file_name = representation;
		file_name += "__";
		file_name += task;
		file_name += "__";
		file_name += date_time;
		file_name += ".csv";


		// Open the file

		FILE* f = fopen(file_name.c_str(), "w");
		if (f == NULL) {
			perror("fopen");
			abort();
		}


		// Write the meta-data

		std::ostringstream header;
		std::ostringstream data;

		data << std::setprecision(6) << std::fixed;

		header << "representation,specialized";
		data << representation;
#if BENCHMARK_TASK_ID >= 0
		data << ",true";
#else
		data << ",false";
#endif

		header << ",year,month,day,hour,min,sec";
		data << "," << tm_start_time.tm_year + 1900;
		data << "," << tm_start_time.tm_mon + 1;
		data << "," << tm_start_time.tm_mday;
		data << "," << tm_start_time.tm_hour;
		data << "," << tm_start_time.tm_min;
		data << "," << tm_start_time.tm_sec;

		header << ",load_time";
		if (load_times.empty()) data << ",0";
		else data << "," << ll_mean(load_times);

		header << ",graph_max_nodes,graph_levels";
		data << G.max_nodes();
		data << G.num_levels();

		fprintf(f, "%s\n%s\n\n", header.str().c_str(), data.str().c_str());


		// Write the summary results

		header.str("");
		data.str("");

		header << "name";
		data << task;

		header << ",runtime_ms,stdev_ms,min_ms,max_ms,c95_ms";
		data << "," << ll_mean(runtimes);
		data << "," << ll_stdev(runtimes);
		data << "," << ll_min(runtimes);
		data << "," << ll_max(runtimes);
		data << "," << ll_c95(runtimes);

		header << ",cpu_ms,cpu_util,major_faults,in_blocks,out_blocks";
		data << "," << ll_mean(cpu_times);
		data << "," << ll_mean(cpu_util);
		data << "," << ll_mean(major_faults);
		data << "," << ll_mean(in_blocks);
		data << "," << ll_mean(out_blocks);

		fprintf(f, "%s\n%s\n\n", header.str().c_str(), data.str().c_str());


		// Write the individual operations

		header.str("");
		header << "id,runtime_ms,cpu_ms,cpu_util";
		header << ",major_faults,in_blocks,out_blocks";

		fprintf(f, "%s\n", header.str().c_str());

		for (size_t i = 0; i < runtimes.size(); i++) {
			data.str("");
			data << i << "," << runtimes[i];
			data << "," << cpu_times[i];
			data << "," << cpu_util[i];
			data << "," << major_faults[i];
			data << "," << in_blocks[i];
			data << "," << out_blocks[i];
			fprintf(f, "%s\n", data.str().c_str());
		}


		// Close

		fclose(f);
	}
};


/**
 * The benchmark repeat count and current state
 */
class ll_benchmark_counter {

public:

	size_t benchmark_count;
	size_t current_iteration;		// within benchmark_count

	size_t max_batches;
	size_t current_batch;

	bool print_progress;
	bool verbose;


public:

	/**
	 * Create an instance of ll_benchmark_counter
	 */
	ll_benchmark_counter(void) {

		benchmark_count = 1;
		current_iteration = 0;

		max_batches = 0;
		current_batch = 0;

		print_progress = true;
		verbose = false;
	}


	/**
	 * Print the stuff before a batch
	 */
	void print_before_batch() {
		if (print_progress && verbose) {
			fprintf(stderr, "\r%5lu: Loading...", current_batch+1);
		}
	}


	/**
	 * Print the stuff after a batch
	 *
	 * @param stats the stats
	 */
	void print_after_batch_load(const ll_benchmark_stats& stats) {
		if (print_progress && verbose) {
			fprintf(stderr, "\r%5lu:           \b\b\b\b\b\b\b\b\b\b",
					current_batch);
#ifndef BENCHMARK_CONCURRENT_LOAD
			if (benchmark_count > 1) {
#endif
				double load_time
					= stats.load_times[stats.load_times.size()-1];
				double load_pull
					= stats.load_pull_times[stats.load_pull_times.size()-1];
				double load_cp
					= stats.load_cp_times[stats.load_cp_times.size()-1];
				double load_delete
					= stats.delete_snapshot_times.empty() ? 0
					: stats.delete_snapshot_times
					[stats.delete_snapshot_times.size()-1];
				print_time(stderr, "Load: ", load_time, false);
				fprintf(stderr, " (%0.3lf pull, %0.3lf cp, %0.3lf delete)\n",
						load_pull/1000.0, load_cp/1000.0,
						load_delete/1000.0);
#ifndef BENCHMARK_CONCURRENT_LOAD
			}
#endif
		}
	}


	/**
	 * Print the stuff before a benchmark
	 */
	void print_before_benchmark() {

		if (print_progress) {
			if (verbose) {
#ifdef LL_STREAMING
				if (benchmark_count > 1)
					fprintf(stderr, "\r%5lu - %lu/%lu: ",
							current_batch, current_iteration+1,
							benchmark_count);
				else
					fprintf(stderr, "\r%5lu: ", current_batch);
#else
				fprintf(stderr, "\r%3lu/%lu: ", current_iteration+1,
						benchmark_count);
#endif
			}
			else {
				fprintf(stderr, ".");
				if ((current_iteration + 1) % 10 == 0) {
					fprintf(stderr, "%lu", current_iteration + 1);
				}
			}
		}
	}


	/**
	 * Print the stuff after a benchmark
	 *
	 * @param stats the stats
	 */
	void print_after_benchmark(const ll_benchmark_stats& stats) {

		if (print_progress && verbose) {
			double t = stats.runtimes[stats.runtimes.size()-1];
#ifdef LL_STREAMING
#	ifndef BENCHMARK_CONCURRENT_LOAD
			if (current_iteration == 0 && benchmark_count == 1) {
				double load_time
					= stats.load_times[stats.load_times.size()-1];
				double load_pull
					= stats.load_pull_times[stats.load_pull_times.size()-1];
				double load_cp
					= stats.load_cp_times[stats.load_cp_times.size()-1];
				double load_delete
					= stats.delete_snapshot_times.empty() ? 0
					: stats.delete_snapshot_times
					[stats.delete_snapshot_times.size()-1];
				print_time(stderr, "Load: ", load_time, false);
				fprintf(stderr, " (%0.3lf pull, %0.3lf cp, %0.3lf delete)",
						load_pull/1000.0, load_cp/1000.0,
						load_delete/1000.0);
				print_time(stderr, ", Run: ", t);
			}
			else {
				print_time(stderr, "Run: ", t);
			}
#	else
			print_time(stderr, "Run: ", t);
#	endif
#else
			print_time(stderr, "", t);
#endif
		}
	}


	/**
	 * Print stuff at the end
	 */
	void print_at_end() {
		if (print_progress && !verbose) {
			fprintf(stderr, "\n");
		}
	}
};


//==========================================================================//
// Things to run: Benchmarks, tools, and tests                              //
//==========================================================================//

struct ll_runnable_thing {
	const char* rt_class;
	const char* rt_identifier;
	const char* rt_name;
	bool rt_needs_reverse_edges;
};

static ll_runnable_thing g_runnable_things[] = {
	{ "ll_b_avg_teen_cnt"         , "avg_teen_cnt"
	                              , "Average teen friends count"
	                              , true  },
	{ "ll_b_bc_adj"               , "bc_adj"
	                              , "Betweenness centrality - exact"
	                              , false },
	{ "ll_b_bc_random"            , "bc_random"
	                              , "Betweenness centrality - randomized"
	                              , false },
	{ "ll_b_bfs"                  , "bfs_count"
	                              , "Breadth-first search - count vertices"
	                              , false },
	{ "ll_b_pagerank_pull_float"  , "pagerank"
	                              , "PageRank - pull version"
	                              , true  },
	{ "ll_b_pagerank_push_float"  , "pagerank_push"
	                              , "PageRank - push version"
	                              , false },
	{ "ll_b_sssp_weighted"        , "sssp_weighted"
	                              , "Weighted SSSP"
	                              , false },
	{ "ll_b_sssp_unweighted_bfs"  , "sssp_unweighted"
	                              , "Unweighted SSSP - BFS"
	                              , false },
	{ "ll_b_tarjan_scc"           , "tarjan_scc"
	                              , "Tarjan's SCC algorithm"
	                              , false },
	{ "ll_b_triangle_counting_LI" , "tc_i"
	                              , "Triangle counting for graph loaded with -I"
	                              , true  },
	{ "ll_b_triangle_counting_LOD", "tc_od"
	                              , "Triangle counting for graph loaded with -OD"
	                              , false },
	{ "ll_b_triangle_counting_LU" , "tc_u"
	                              , "Triangle counting for graph loaded with -U"
	                              , false },
	{ "ll_t_delete_edges"         , "t:delete_edges"
	                              , "Regression test: delete edges"
	                              , true  },
	{ "ll_t_delete_nodes"         , "t:delete_nodes"
	                              , "Regression test: delete nodes"
	                              , true  },
	{ "ll_t_level_spread"         , "level_spread"
	                              , "Compute the level spread"
	                              , false },
	{ "ll_t_degree_distribution"  , "degree_distribution"
	                              , "Compute the degree distribution"
	                              , false },
	{ "ll_t_flatten"              , "flatten"
	                              , "Flatten (fully merge) the graph"
	                              , false },
	{ "ll_t_dump"                 , "dump"
	                              , "Dump the graph"
	                              , false },
	{ "ll_t_edge_property_stats"  , "edge_prop_stats"
	                              , "Edge property stats"
	                              , false },
	{ "ll_b_pagerank_pull_double" , "pagerank_double"
	                              , "PageRank - pull version, double"
	                              , true  },
	{ "ll_b_pagerank_push_double" , "pagerank_double_push"
	                              , "PageRank - push version, double"
	                              , false },
	{ "ll_b_sssp_unweighted_iter" , "sssp_unweighted_iter"
	                              , "Unweighted SSSP - iterative"
	                              , false },
	{ NULL, NULL, NULL, false }
};


#ifndef BENCHMARK_TASK_ID
#define BENCHMARK_TASK_ID -1
#endif

#ifndef BENCHMARK_TASK_STR
#define BENCHMARK_TASK_STR NULL
#endif

#define LL_RT_COND_CREATE(var, index, class_name, ...) { \
	assert(strcmp(g_runnable_things[index].rt_class, #class_name) == 0); \
	if (strcmp(var, #class_name) == 0) \
		benchmark = new class_name<benchmarkable_graph_t>(__VA_ARGS__); \
}

#define LL_RT_COND_CREATE_EXT(var, index, class_name, template_parameter, ...) { \
	assert(strcmp(g_runnable_things[index].rt_class, #class_name) == 0); \
	if (strcmp(var, #class_name) == 0) \
		benchmark = new class_name<benchmarkable_graph_t, \
							template_parameter>(__VA_ARGS__); \
}



//==========================================================================//
// The Command-Line Arguments                                               //
//==========================================================================//

static const char* SHORT_OPTIONS = "c:C:d:DIhl:Ln:No:OP:r:R:t:ST:UvX:"
	IF_LL_STREAMING("B:M:W:");

static struct option LONG_OPTIONS[] =
{
	{"count"        , required_argument, 0, 'c'},
	{"compare"      , required_argument, 0, 'C'},
	{"database"     , required_argument, 0, 'd'},
	{"deduplicate"  , no_argument      , 0, 'D'},
	{"help"         , no_argument,       0, 'h'},
	{"in-edges"     , no_argument,       0, 'I'},
	{"level"        , required_argument, 0, 'l'},
	{"levels"       , required_argument, 0, 'l'},
	{"load"         , no_argument,       0, 'L'},
	{"num-iters"    , required_argument, 0, 'n'},
	{"no-properties", no_argument,       0, 'N'},
	{"output"       , required_argument, 0, 'o'},
	{"print"        , required_argument, 0, 'P'},
	{"run"          , required_argument, 0, 'r'},
	{"root"         , required_argument, 0, 'R'},
	{"save-stats"   , no_argument,       0, 'S'},
	{"threads"      , required_argument, 0, 't'},
	{"temp-dir"     , required_argument, 0, 'T'},
	{"undir-double" , no_argument,       0, 'U'},
	{"undir-order"  , no_argument,       0, 'O'},
	{"verbose"      , no_argument,       0, 'v'},
	{"xs-buffer"    , required_argument, 0, 'X'},
#ifdef LL_STREAMING
	{"batch"        , required_argument, 0, 'B'},
	{"max-batches"  , required_argument, 0, 'M'},
	{"window"       , required_argument, 0, 'W'},
#endif
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
#ifdef LL_STREAMING
	fprintf(stderr, "  -B, --batch N         Set the batch size for advancing the stream\n");
#endif
	fprintf(stderr, "  -c, --count N         Run the given benchmark operation N times\n");
	fprintf(stderr, "  -C, --compare FILE    Compare graph to the one in the given file\n");
	fprintf(stderr, "  -d, --database DIR    Set the database directory\n");
	fprintf(stderr, "  -D, --deduplicate     Deduplicate edges within level while loading\n");
	fprintf(stderr, "  -h, --help            Show this usage information and exit\n");
	fprintf(stderr, "  -I, --in-edges        Load or generate in-edges\n");
	fprintf(stderr, "  -l, --level N[-M]     Set the level or the min and max levels\n");
#ifdef LL_PERSISTENCE
	fprintf(stderr, "  -L, --load            Load the input files into the database\n");
#endif
#ifdef LL_STREAMING
	fprintf(stderr, "  -M, --max-batches M   Set the maximum number of batches\n");
#endif
	fprintf(stderr, "  -n, --num-iters N     Set the number of iterations (if applicable)\n");
	fprintf(stderr, "  -N, --no-properties   Do not load (ingest) properties\n");
	fprintf(stderr, "  -o, --output FILE     Write the query output to a file\n");
	fprintf(stderr, "  -O, --undir-order     Load undirected by ordering all edges\n");
	fprintf(stderr, "  -P, --print N[-M]     Print edges adjacent to one or more nodes\n");
#if BENCHMARK_TASK_ID < 0
	fprintf(stderr, "  -r, --run TASK        Run a task\n");
#endif
	fprintf(stderr, "  -R, --root N          Set the root node for SSSP and BFS\n");
	fprintf(stderr, "  -S, --save-stats      Save the execution statistics\n");
	fprintf(stderr, "  -t, --threads N       Set the number of threads\n");
	fprintf(stderr, "  -T, --temp DIR        Add a temporary directory\n");
	fprintf(stderr, "  -U, --undir-double    Load undirected by doubling all edges\n");
	fprintf(stderr, "  -v, --verbose         Enable verbose output\n");
#ifdef LL_STREAMING
	fprintf(stderr, "  -W, --window N        Set the sliding window size to be N batches\n");
#endif
	fprintf(stderr, "  -X, --xs-buffer GB    Set the external sort buffer size, in GB\n");

	fprintf(stderr, "\nTasks (run using the --run/-r option):\n");

	int longest_identifier_length = 0;
	for (ll_runnable_thing* rt = g_runnable_things; rt->rt_class != NULL; rt++) {
		int l = (int) strlen(rt->rt_identifier);
		if (l > longest_identifier_length) longest_identifier_length = l;
	}

	std::vector<std::string> l;
	char buffer[256];
	for (ll_runnable_thing* rt = g_runnable_things; rt->rt_class != NULL; rt++) {
		snprintf(buffer, 255, "  %-*s  %s", longest_identifier_length,
				rt->rt_identifier, rt->rt_name);
		l.push_back(buffer);
	}

	std::stable_sort(l.begin(), l.end());
	for (size_t i = 0; i < l.size(); i++) {
		fprintf(stderr, "%s\n", l[i].c_str());
	}
}




//==========================================================================//
// Benchmark Components                                                     //
//==========================================================================//

/**
 * Load a part of the graph using the writable representation
 * 
 * @param graph the writable graph
 * @param data_source the data source
 * @param loader_config the loader configuration
 * @param batch_size the batch size (the max number of edges to load)
 * @param window_size the streaming window size (if streaming)
 * @param stats the stats
 * @return true if the graph was loaded
 */
bool load_batch_via_writable_graph(ll_writable_graph& graph,
		ll_data_source& data_source, const ll_loader_config& loader_config,
		size_t batch_size, ssize_t window_size, ll_benchmark_stats& stats) {

	stats.before_load();

	stats.before_load_pull();
	bool loaded = data_source.pull(&graph, batch_size);
	stats.after_load_pull();
	if (!loaded) return false;

	stats.before_load_cp();
	graph.checkpoint(&loader_config);
	stats.after_load_cp();

#ifdef LL_STREAMING
	if (window_size > 0 && graph.num_levels() >= (size_t) window_size) {
		graph.set_min_level(graph.num_levels() - window_size);
		if (graph.num_levels() >= (size_t) window_size + 2) {
			stats.before_delete_snapshot();
			graph.delete_level(graph.num_levels() - window_size - 2);
			stats.after_delete_snapshot();
		}
	}
#endif

	stats.after_load();

	return true;
}


/**
 * Run the benchmark on the given graph
 *
 * @param graph the graph
 * @param benchmark the benchmark
 * @param stats the stats
 * @return the return code
 */
template <class Graph>
double run_benchmark(Graph& graph, ll_benchmark<Graph>* benchmark,
		ll_benchmark_stats& stats) {

	benchmark->initialize(&graph);

	stats.before_benchmark();
	double return_d = benchmark->run();
	stats.after_benchmark();

	double r_d_adj = benchmark->finalize();
	if (!std::isnan(r_d_adj)) return_d = r_d_adj;

	return return_d;
}




//==========================================================================//
// The Main Function                                                        //
//==========================================================================//

/**
 * The main function
 */
int main(int argc, char** argv)
{
	char* cross_validate_with = NULL;
	char* database_directory = NULL;
	char* run_task = NULL;
	char* output_file = NULL;

	bool verbose = false;
	bool save_execution_statistics = false;

	ll_benchmark_counter counter;

	int min_level = 0;
	int max_level = -1;
	int num_threads = -1;
	int num_iters = -1;

	node_t root_node = 0; (void) root_node;
	node_t print_node_from = LL_NIL_NODE;
	node_t print_node_to = LL_NIL_NODE;

	bool do_load = false;
	bool do_in_edges = false;
	ll_loader_config loader_config;

	int concurrent_load_threads = 1; (void) concurrent_load_threads;

	int streaming_batch = 1000 * 1000; (void) streaming_batch;
	int streaming_window = 10; (void) streaming_window;


	// Pase the command-line arguments

	int option_index = 0;
	while (true) {
		int c = getopt_long(argc, argv, SHORT_OPTIONS, LONG_OPTIONS, &option_index);

		if (c == -1) break;

		switch (c) {

			case 'B':
				streaming_batch = atoi(optarg);
				if (streaming_batch <= 0) {
					fprintf(stderr, "Error: The batch size must be positive\n");
					return 1;
				}
				break;

			case 'c':
				counter.benchmark_count = atoi(optarg);
				break;

			case 'C':
				cross_validate_with = optarg;
				break;

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
				do_in_edges = true;
				break;

			case 'l':
				if (strchr(optarg, '-') == NULL) {
					max_level = atoi(optarg);
				}
				else {
					char* x = strdup(optarg);
					char* y = strchr(x, '-');
					*(y++) = '\0';
					min_level = atoi(x);
					max_level = atoi(y);
					free(x);
				}
				break;

			case 'L':
				do_load = true;
				break;

			case 'n':
				num_iters = atoi(optarg);
				break;

			case 'N':
				loader_config.lc_no_properties = true;
				break;

			case 'M':
				counter.max_batches = atoi(optarg);
				break;

			case 'o':
				output_file = optarg;
				break;

			case 'O':
				loader_config.lc_direction = LL_L_UNDIRECTED_ORDERED;
				break;

			case 'P':
				if (strchr(optarg, '-') == NULL) {
					print_node_from = print_node_to = atoi(optarg);
				}
				else {
					char* x = strdup(optarg);
					char* y = strchr(x, '-');
					*(y++) = '\0';
					print_node_from = atoi(x);
					print_node_to = atoi(y);
					free(x);
				}
				break;

			case 'r':
				run_task = optarg;
				break;

			case 'R':
				root_node = atoi(optarg);
				break;

			case 'S':
				save_execution_statistics = true;
				break;

			case 't':
				num_threads = atoi(optarg);
				break;

			case 'T':
				loader_config.lc_tmp_dirs.push_back(std::string(optarg));
				break;

			case 'U':
				loader_config.lc_direction = LL_L_UNDIRECTED_DOUBLE;
				break;

			case 'v':
				verbose = true;
				break;

			case 'W':
				streaming_window = atoi(optarg);
				if (streaming_window <= 0) {
					fprintf(stderr, "Error: The window size must be positive\n");
					return 1;
				}
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

	counter.verbose = verbose;

	std::vector<std::string> input_files;
	for (int i = optind; i < argc; i++) {
		input_files.push_back(std::string(argv[i]));
	}

#ifdef LL_PERSISTENCE
	if (do_load && input_files.empty()) {
		fprintf(stderr, "Error: No input files specified (use -h for help).\n");
		return 1;
	}
	if (!do_load && !input_files.empty()) {
		fprintf(stderr, "Error: Input files specified without the --load/-L argument.\n");
		return 1;
	}
#else
	if (input_files.empty()) {
		fprintf(stderr, "Error: No input files specified (use -h for help).\n");
		return 1;
	}
#endif

	if (database_directory == NULL) {
		database_directory = (char*) alloca(16);
		strcpy(database_directory, "db");
	}

	if (max_level < 0) max_level = input_files.size() - 1;


	// Get the task/operation to run

	const char* run_task_class = "";
	bool needs_reverse_edges = do_in_edges;
	ll_runnable_thing* run = NULL;

#if BENCHMARK_TASK_ID >= 0

	run = &g_runnable_things[BENCHMARK_TASK_ID];
	run_task_class = run->rt_class;
	if (run->rt_needs_reverse_edges) needs_reverse_edges = true;

	run_task = NULL;
	(void) run_task;
	(void) do_load;

#else

	if (run_task != NULL) {
		for (ll_runnable_thing* rt = g_runnable_things; rt->rt_class != NULL; rt++) {
			if (strcmp(run_task, rt->rt_identifier) == 0) {
				run = rt;
				run_task_class = rt->rt_class;
				if (rt->rt_needs_reverse_edges) needs_reverse_edges = true;
				break;
			}
		}
		if (*run_task_class == '\0') {
			fprintf(stderr, "Error: Unknown task (use -h for help).\n");
			return 1;
		}
	}
	else if (!do_load) {
		fprintf(stderr, "Error: No task to run (use -h for help).\n");
		return 1;
	}

#endif


	// Check the files' type consistency

	const char* file_type = NULL;
	const char* first_input = NULL;

	if (!input_files.empty()) {

		first_input = input_files[0].c_str();
		file_type = ll_file_extension(first_input);

		for (size_t i = 1; i < input_files.size(); i++) {
			if (strcmp(ll_file_extension(input_files[i].c_str()), file_type)!=0) {
				fprintf(stderr, "Error: All imput files must have the same "
						"file extension.\n");
				return 1;
			}
		}

		if (cross_validate_with != NULL) {
			if (strcmp(ll_file_extension(cross_validate_with), file_type) != 0) {
				fprintf(stderr, "Error: All imput files must have the same "
						"file extension.\n");
				return 1;
			}
		}
	}
	else if (cross_validate_with != NULL) {
		file_type = ll_file_extension(cross_validate_with);
	}

	
	//
	//
	// Init
	//
	//
	
	if (verbose) {
		fprintf(stderr, "LLAMA BENCHMARK COLLECTION\n\n");
#if BENCHMARK_TASK_ID < 0
		fprintf(stderr, "IMPORTANT: Please compile and run the individual "
				"benchmark tasks\nusing \"make TASK=...\" in order to get "
				"better performance results.\n\n");
#endif
	}


	// Create the benchmark

	ll_benchmark<benchmarkable_graph_t>* benchmark = NULL;

	int pagerank_iters = num_iters < 0 ? 10 : num_iters;
	(void) pagerank_iters;

#define B BENCHMARK_TASK_ID
#if B < 0 || B == 0
	LL_RT_COND_CREATE(run_task_class,  0, ll_b_avg_teen_cnt, 0);
#endif
#if B < 0 || B == 1
	LL_RT_COND_CREATE(run_task_class,  1, ll_b_bc_adj);
#endif
#if B < 0 || B == 2
	LL_RT_COND_CREATE(run_task_class,  2, ll_b_bc_random, 100);
#endif
#if B < 0 || B == 3
	LL_RT_COND_CREATE(run_task_class,  3, ll_b_bfs, root_node);
#endif
#if B < 0 || B == 4
	LL_RT_COND_CREATE(run_task_class,  4, ll_b_pagerank_pull_float, pagerank_iters);
#endif
#if B < 0 || B == 5
	LL_RT_COND_CREATE(run_task_class,  5, ll_b_pagerank_push_float, pagerank_iters);
#endif
#if B < 0 || B == 6
	LL_RT_COND_CREATE_EXT(run_task_class,  6, ll_b_sssp_weighted, float,
			root_node, "weight");
#endif
#if B < 0 || B == 7
	LL_RT_COND_CREATE(run_task_class,  7, ll_b_sssp_unweighted_bfs, root_node);
#endif
#if B < 0 || B == 8
	LL_RT_COND_CREATE(run_task_class,  8, ll_b_tarjan_scc);
#endif
#if B < 0 || B == 9
	LL_RT_COND_CREATE(run_task_class,  9, ll_b_triangle_counting_LI);
#endif
#if B < 0 || B == 10
	LL_RT_COND_CREATE(run_task_class, 10, ll_b_triangle_counting_LOD);
#endif
#if B < 0 || B == 11
	LL_RT_COND_CREATE(run_task_class, 11, ll_b_triangle_counting_LU);
#endif
#if B < 0 || B == 12
# ifdef BENCHMARK_WRITABLE
	LL_RT_COND_CREATE(run_task_class, 12, ll_t_delete_edges);
# endif
#endif
#if B < 0 || B == 13
# ifdef BENCHMARK_WRITABLE
	LL_RT_COND_CREATE(run_task_class, 13, ll_t_delete_nodes);
# endif
#endif
#if B < 0 || B == 14
	LL_RT_COND_CREATE(run_task_class, 14, ll_t_level_spread);
#endif
#if B < 0 || B == 15
	LL_RT_COND_CREATE(run_task_class, 15, ll_t_degree_distribution);
#endif
#if B < 0 || B == 16
	LL_RT_COND_CREATE(run_task_class, 16, ll_t_flatten, "db-m");
#endif
#if B < 0 || B == 17
	LL_RT_COND_CREATE(run_task_class, 17, ll_t_dump);
#endif
#if B < 0 || B == 18
	LL_RT_COND_CREATE_EXT(run_task_class, 18, ll_t_edge_property_stats,
			uint32_t, "stream-weight");
#endif
#if B < 0 || B == 19
	LL_RT_COND_CREATE(run_task_class, 19, ll_b_pagerank_pull_double, pagerank_iters);
#endif
#if B < 0 || B == 20
	LL_RT_COND_CREATE(run_task_class, 20, ll_b_pagerank_push_double, pagerank_iters);
#endif
#if B < 0 || B == 21
	LL_RT_COND_CREATE(run_task_class, 21, ll_b_sssp_unweighted_iter, root_node);
#endif
#undef B

	if (benchmark == NULL) counter.benchmark_count = 0;


	// Preallocate some writable objects

	// TODO This should be a bit more customizable
	
#ifndef LL_WRITABLE_USE_MEMORY_POOL

	if (false) {
		w_node_deallocator wdn;
		w_edge_deallocator wde;

		int nc = 1048576, ec = 1048576;
#if defined(_DEBUG) || !(defined(DO_CP) || defined(BENCHMARK_WRITABLE))
		nc = ec = 4;
#endif

		LL_D_PRINT("Preallocating %d nodes (x %lu bytes) and %d edges (x %lu bytes)\n",
				nc, sizeof(w_node), ec, sizeof(w_edge));

		while (ec --> 0) wde(new w_edge());
		while (nc --> 0) wdn(new w_node());
	}

#endif


	// Counters and helper variables

	double return_d = 0;
	ll_benchmark_stats stats;


	// Print the header

	if (benchmark != NULL) {

		char s_start_time[64];
		strftime(s_start_time, sizeof(s_start_time), "%m/%d/%y %H:%M:%S",
				&stats.tm_start_time);

		printf("Benchmark  : %s\n", benchmark->name());
		printf("Start Time : %s\n", s_start_time);
		printf("Count      : %lu\n", counter.benchmark_count);
		fflush(stdout);
	}


	// Prepare the benchmark

	stats.before_load();

	bool ll = true; (void) ll;
	loader_config.lc_reverse_edges = needs_reverse_edges;
#ifdef BENCHMARK_WRITABLE
	ll = false;
	loader_config.lc_reverse_maps = needs_reverse_edges;
#else
#ifdef DO_CP
	ll = false;
#endif
#endif

	ll_database database(database_directory);
	if (num_threads > 0) database.set_num_threads(num_threads);
	ll_writable_graph& graph = *database.graph();//(max_nodes);


#ifndef LL_STREAMING

	// Load the graph

	if (!input_files.empty()) {

		ll_file_loaders loaders;
		ll_file_loader* loader = loaders.loader_for(first_input);
		if (loader == NULL) {
			fprintf(stderr, "Error: Unsupported input file type\n");
			return 1;
		}

		if (verbose) fprintf(stderr, "\nLoading:\n");
		for (int i = 0; i <= max_level; i++) {
			if (verbose) fprintf(stderr, " %2d: %s", i, input_files[i].c_str());
			double ts = ll_get_time_ms();
#ifdef DO_CP
			bool loadRO = false;
#else
			bool loadRO = i == max_level ? ll : true;
#endif

			if (loadRO)
				loader->load_direct(&graph, input_files[i].c_str(), &loader_config);
			else
				loader->load_incremental(&graph, input_files[i].c_str(), &loader_config);

			double t_l = ll_get_time_ms() - ts;
			double tt = t_l;
			if (verbose) fprintf(stderr, " (Load: %3.2lf s", t_l/1000.0);

#ifdef DO_CP
			ts = ll_get_time_ms();
#	ifdef BENCHMARK_WRITABLE
			if (i < max_level) graph.checkpoint(&loader_config);
#	else
			graph.checkpoint(&loader_config);
#	endif
			double t_c = ll_get_time_ms() - ts;
			tt += t_c;
			if (verbose) {
				fprintf(stderr, ", CP: %3.2lf s", t_c/1000.0);
			}
#endif
			if (verbose) {
				fprintf(stderr, ", %7.2lf Kedges/s)\n",
						graph.max_edges(graph.num_levels() - 2) / tt);
			}
		}
		//if (verbose) fprintf(stderr, "\n");

		stats.after_load();
	}

#endif


	// Prepare the graph

	benchmarkable_graph_t& G = benchmarkable_graph(graph);

	if (input_files.empty() && max_level < 0) {
		max_level = G.num_levels() - 1;
	}

#ifdef LL_PERSISTENCE
	//XXX
	/*if (input_files.empty()) {
		graph.ro_graph().create_uninitialized_edge_property_32("weight", LL_T_FLOAT)
			->init_from_persistent_storage(G.ro_graph().num_levels());
	}*/
#endif

#ifdef LL_MIN_LEVEL
	// Set the min level

	if (min_level > 0) {
		G.set_min_level(min_level);
		for (int i = 0; i + 1 < min_level; i++) {
			graph.delete_level(i);
		}
	}
#endif


	// Print a part of the graph
	
	if (print_node_from <= print_node_to
			&& print_node_from != LL_NIL_NODE) {
		printf("\n");
		printf("Number of nodes : %lu\n", (size_t) G.max_nodes());
		if (G.num_levels() == 1) printf("Number of edges : %lu\n", (size_t) G.max_edges(0));
		printf("Number of levels: %lu\n", (size_t) G.num_levels());
		printf("\n");
		printf("A part of the graph:\n");
		for (node_t n = print_node_from; n <= print_node_to; n++) {
			printf("%7u [out]:", (unsigned) n); print_exp_adj_out(G, n);
			if (needs_reverse_edges) {
				printf("%7u [ in]:", (unsigned) n); print_exp_adj_in(G, n);
			}
		}
		printf("\n");
	}


	// The test function for disassembly analysis (do not run)

	if (1 + ((int) (rand() & 3)) == 0) TEST_FUNCTION(G);


	// Cross-validate with the provided file

	if (cross_validate_with != NULL) {
		if (verbose) {
			fprintf(stderr, "\n");
		}
		int r = cross_validate_with_file(graph, cross_validate_with,
				file_type, verbose, &loader_config);
		if (r != 0) return r;
		if (verbose) {
			fprintf(stderr, "\n");
		}
	}


	//
	//
	// Run the benchmark
	//
	//

	if (counter.print_progress && counter.benchmark_count > 0) {
		benchmark->set_print_progress(verbose);

		if (verbose) {
			fprintf(stderr, "\nRunning:\n");
		}
		else {
			fprintf(stderr, "\nProgress: ");
		}
	}


#ifdef BENCHMARK_CONCURRENT_LOAD
	int concurrent_task_threads
		= (num_threads > 0 ? num_threads : omp_get_max_threads())
		- concurrent_load_threads;

	if (concurrent_task_threads <= 0) {
		fprintf(stderr, "Error: No threads left for execution\n");
		return 1;
	}

    omp_set_num_threads(2);		// Probably not necessary
    omp_set_nested(1);
#endif


#if defined(LL_STREAMING)

	ll_file_loaders loaders;
	ll_file_loader* loader = loaders.loader_for(input_files[0].c_str());
	if (loader == NULL) {
		fprintf(stderr, "Error: Unsupported input file type\n");
		return 1;
	}

	ll_concat_data_source combined_data_source;
	for (int i = 0; i <= max_level; i++) {
		ll_data_source* d = loader->create_data_source(input_files[i].c_str());
		combined_data_source.add(d);
	}

	counter.current_batch = 0;

	while (true) {

#	if defined(BENCHMARK_CONCURRENT_LOAD)
		
		if (counter.max_batches > 0) {
			if (counter.current_batch > counter.max_batches) break;
		}


		// Initial load

		if (counter.current_batch == 0) {
			
			counter.print_before_batch();
			if (!load_batch_via_writable_graph(graph, combined_data_source,
						loader_config, streaming_batch, streaming_window,
						stats)) break;
			counter.current_batch++;
			counter.print_after_batch_load(stats);
		}


		// The concurrent sections

		// TODO Move CP and delete to the task execution section

		volatile bool loaded = false;
		ll_mlcsr_ro_graph G_ro(&G);

		counter.print_before_batch();

#		pragma omp parallel sections
		{

			// The load section

#			pragma omp section
			{
				omp_set_num_threads(concurrent_load_threads);

				// TODO Check if this does the right thing
				//      for concurrent_load_threads != 1

				if (counter.current_batch < counter.max_batches
						|| counter.max_batches <= 0) {
					loaded = load_batch_via_writable_graph(graph,
							combined_data_source, loader_config,
							streaming_batch, streaming_window, stats);
				}
			}


			// The task execution section

#			pragma omp section
			{
				omp_set_num_threads(concurrent_task_threads);

				for (counter.current_iteration = 0;
						counter.current_iteration < counter.benchmark_count;
						counter.current_iteration++) {

#ifdef LL_COUNTERS
					ll_clear_counters();
#endif

					counter.print_before_benchmark();
					return_d = run_benchmark(G_ro, benchmark, stats);
					counter.print_after_benchmark(stats);
				}
			}
		}

		counter.current_batch++;
		if (!loaded) break;

		counter.print_after_batch_load(stats);


#	else	/* !defined(BENCHMARK_CONCURRENT_LOAD) */
		
		if (counter.max_batches > 0) {
			if (counter.current_batch >= counter.max_batches) break;
		}

		counter.print_before_batch();
		if (!load_batch_via_writable_graph(graph, combined_data_source,
				loader_config, streaming_batch, streaming_window,
				stats)) break;
		counter.current_batch++;
		counter.print_after_batch_load(stats);

		for (counter.current_iteration = 0;
				counter.current_iteration < counter.benchmark_count;
				counter.current_iteration++) {

#ifdef LL_COUNTERS
			ll_clear_counters();
#endif

			counter.print_before_benchmark();
			return_d = run_benchmark(G, benchmark, stats);
			counter.print_after_benchmark(stats);
		}

#	endif

	}

#else /* if !defined(LL_STREAMING) */

#	if defined(BENCHMARK_CONCURRENT_LOAD)
#		error "Not implemented"
#	else

	for (counter.current_iteration = 0;
			counter.current_iteration < counter.benchmark_count;
			counter.current_iteration++) {

#ifdef LL_COUNTERS
		ll_clear_counters();
#endif

		counter.print_before_benchmark();
		return_d = run_benchmark(G, benchmark, stats);
		counter.print_after_benchmark(stats);
	}

#	endif
#endif

	counter.print_at_end();

#if defined(LL_SLCSR)
	const char* type = "slcsr";
#elif defined(LL_PERSISTENCE)
	const char* type = "persistence";
#elif defined(LL_MEMORY_ONLY)
	const char* type = "memory";
#elif defined(LL_STREAMING)
	const char* type = "streaming";
#else
#error "Unknown configuration"
#endif

	std::string configuration_summary = type;

#ifdef BENCHMARK_CONCURRENT_LOAD
	configuration_summary += "_conc";
#endif

#if defined(LL_ONE_VT)
	configuration_summary += "_onevt";
#elif defined(LL_FLAT_VT)
	configuration_summary += "_flatvt";
#endif

#ifdef LL_MLCSR_CONTINUATIONS
	configuration_summary += "_cont";
#endif

#ifdef BENCHMARK_WRITABLE
	configuration_summary += "_w";
#else
#  ifdef LL_DELETIONS
	configuration_summary += "_wd";
#  endif
#endif

#if BENCHMARK_TASK_ID >= 0
	//configuration_summary += "_specialized";
	configuration_summary += "_s-";
	configuration_summary += run->rt_identifier;
#endif

	printf("\nNode type  : %d-bit\nEdge type  : %d-bit\n",
			(int) sizeof(node_t) * 8, (int) sizeof(edge_t) * 8);
	printf("Deletions  : %s\n", IFE_LL_DELETIONS("yes", "no"));
	printf("Levels     : %d--%d%s\n", min_level, max_level,
#ifdef BENCHMARK_WRITABLE
			" (W)"
#else
			""
#endif
		  );
	printf("Type       : %s\n\n", configuration_summary.c_str());
	printf("# Nodes    : %lu\n", (size_t) graph.max_nodes());
	if (graph.ro_graph().num_levels() == 1)
		printf("# Edges    : %lu\n", (size_t) graph.ro_graph().max_edges(0));
	printf("# Levels   : %lu\n", (size_t) G.num_levels());

	stats.print_stats(stdout);

#ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES
	printf("Edge weights:\n");
	for (int i=0;i<500;i++) {
		uint32_t w = graph.ro_graph().get_edge_weights_streaming()->get(i);
		printf(" %u", w);
		if ((i+1) % 50 == 0) printf("\n");
	}
	printf("\n");
#endif


	// Save the execution statistics
	
	if (benchmark != NULL && save_execution_statistics) {
		stats.save_stats(G, configuration_summary.c_str(), run->rt_identifier);
	}


	// Print the results

	if (benchmark != NULL && output_file != NULL) {

		FILE* f = NULL;
		if (strcmp(output_file, "") == 0 || strcmp(output_file, "-") == 0) {
			f = stdout;
			fprintf(f, "Results:\n");
		}
		else {
			f = fopen(output_file, "w");
			if (f == NULL) {
				perror("fopen");
				abort();
			}
		}

		benchmark->print_results(f);

		if (f != stdout && f != stderr) fclose(f);
	}

	if (!std::isnan((double) return_d)) {
		printf("Return value: %lf\n", return_d);
	}

#ifdef LL_COUNTERS
	fprintf(stderr, "\nCounters:\n");
	ll_print_counters();
#endif

	if (benchmark != NULL) delete benchmark;

	return 0;
}
