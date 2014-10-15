/*
 * ll_external_sort.h
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


#ifndef LL_EXTERNAL_SORT_H_
#define LL_EXTERNAL_SORT_H_

#if defined(__linux__)
#	include <sys/sysinfo.h>
#elif defined(__NetBSD__)
#	include <sys/param.h>
#  	include <sys/sysctl.h>
#	include <uvm/uvm_extern.h>
#elif defined(__APPLE__)
#	include <mach/mach.h>
#else
#endif

#include <sys/time.h>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include <omp.h>

#include "llama/ll_config.h"


// Low-level configuration

//#define LL_XS_DEBUG_PERFORMANCE
//#define LL_XS_HIERARCHICAL_MERGE
#define LL_XS_MULTICORE_SORT


#ifdef LL_XS_DEBUG_PERFORMANCE

/*
 * Get the time in ms for debugging
 */
inline double __get_time_ms() {
	struct timeval TV;
	gettimeofday(&TV, NULL);
	return TV.tv_sec*1000 + TV.tv_usec*0.001;
}

#endif


/**
 * External sort
 */
template <typename T, class Comparator>
class ll_external_sort {

	size_t _size;
	bool _done;
	Comparator _comparator;

	std::vector<std::string> _tmp_dirs;
	int _phase;

	T* _buffer;
	size_t _buffer_size;
	size_t _buffer_capacity;
	size_t _tmp_buffer_capacity;

	std::vector<int> _tmp_files;

#ifndef LL_XS_HIERARCHICAL_MERGE
	std::vector<T*> _tmp_buffers;
	std::vector<size_t> _tmp_buffer_sizes;
	std::vector<size_t> _tmp_buffer_index;
#endif


public:

	/**
	 * Create an instance of ll_external_sort
	 *
	 * @param config the loader configuration
	 */
	ll_external_sort(const ll_loader_config* config = NULL) {

		// Copy the configuration

		ll_loader_config* lc_tmp = config==NULL ? new ll_loader_config() : NULL;
		const ll_loader_config* lc = config == NULL ? lc_tmp : config;

		size_t xs_buffer_size = lc->lc_xs_buffer_size;

		for (size_t i = 0; i < lc->lc_tmp_dirs.size(); i++) {
			const char* d = lc->lc_tmp_dirs[i].c_str();
			if (*d == '\0') {
				_tmp_dirs.push_back("./");
			}
			else {
				std::string s = d;
				if (d[strlen(d)-1] != '/') s += "/";
				_tmp_dirs.push_back(s);
			}
		}

		if (_tmp_dirs.empty()) _tmp_dirs.push_back("/tmp/");

		if (lc_tmp != NULL) delete lc_tmp;


		// Initialize

		_size = 0;
		_done = false;
		_phase = 0;

		_tmp_buffer_capacity = 64 * 1048576ul / sizeof(T);

		if (xs_buffer_size > 0) {
			_buffer_capacity = std::max(1048576ul, xs_buffer_size) / sizeof(T);
		}
		else {

			// Auto-tune

#if defined(__linux__)
			struct sysinfo s;
			if (sysinfo(&s) < 0) {
				perror("sysinfo");
				abort();
			}

			char buf[128];
			FILE* f = fopen("/proc/meminfo", "r");
			for (int i = 0; i <= 3; i++) {
				if (fgets(buf, 128, f) == NULL) {
					perror("fgets");
					abort();
				}
			}
			char *p = strchr(buf, ':');
			size_t cachedram = strtoull(p+1, NULL, 10) * 1024ull;
			fclose(f);

			size_t max = (((size_t) s.mem_unit) * (s.freeram + s.bufferram))
				+ cachedram;

#elif defined(__NetBSD__)

			struct uvmexp_sysctl u;
			size_t u_len = sizeof(u);

			int m[2];
			m[0] = CTL_VM;
			m[1] = VM_UVMEXP2;
			
			if (sysctl(m, 2, &u, &u_len, NULL, 0) != 0) {
				perror("sysctl");
				LL_E_PRINT("Cannot determine the amount of free memory\n");
				abort();
			}

			size_t max = u.pagesize * (u.free + u.filepages);

#elif defined(__APPLE__)

			struct vm_statistics64 s;
			mach_port_t host = mach_host_self();
			natural_t count = HOST_VM_INFO64_COUNT;

			if (host_statistics64(host, HOST_VM_INFO64, (host_info64_t) &s,
						&count) != KERN_SUCCESS) {
				LL_E_PRINT("Cannot determine the amount of free memory\n");
				abort();
			}

			size_t max = getpagesize()
				* ((size_t) s.free_count + s.inactive_count);
#else
#warning "Don't know how to autodetect memory info on this platform"
#warning "(Falling back to a weird C++ trick that probably will not work.)"

			std::pair<char*, std::ptrdiff_t> tmp
				= std::get_temporary_buffer<char>(33ull * 1048576ull * 1048576ull);
			size_t max = tmp.second;
			std::return_temporary_buffer(tmp.first);

			if (max == 33ull * 1048576ull * 1048576ull) {
				LL_E_PRINT("Autodetecting the amount of free memory failed\n");
				LL_W_PRINT("(If you really have > 32 TB RAM, fix this check.)\n");
				abort();
			}
#endif
			//LL_D_PRINT("Detected free memory: %0.2lf MB\n", max/1048576.0);

			if (max < 1048576ul) {
				LL_E_PRINT("Not enough memory: %0.2lf KB\n", max/1024.0);
				abort();
			}

			size_t b = max/2;
#ifdef LL_XS_MULTICORE_SORT
			b /= 2;
#endif
			//LL_I_PRINT("Buffer %0.2lf MB\n", b / 1048576.0);
			_buffer_capacity = b / sizeof(T);
		}

		_buffer_size = 0;
		_buffer = (T*) malloc(sizeof(T) * _buffer_capacity);

		if (_buffer == NULL) {
			if (xs_buffer_size == 0) {
				while (_buffer == NULL) {
					LL_W_PRINT("Not enough memory, trying half\n");
					_buffer_capacity /= 2;
					_buffer = (T*) malloc(sizeof(T) * _buffer_capacity);
				}
			}
			else {
				LL_E_PRINT("Not enough memory\n");
				abort();
			}
		}

#ifdef LL_XS_DEBUG_PERFORMANCE
		fprintf(stderr, "ll_external_sort::ll_external_sort: buffer capacity "
				"= %lu (%0.2lf GB)\n",_buffer_capacity,
				(sizeof(T) * _buffer_capacity) / (1024.0 * 1048576.0));
#endif
	}


	/**
	 * Destroy this instance
	 */
	~ll_external_sort() {

#ifndef LL_XS_HIERARCHICAL_MERGE
		for (size_t i = 0; i < _tmp_buffers.size(); i++) {
			free(_tmp_buffers[i]);
		}
#endif

		free(_buffer);

		for (size_t i = 0; i < _tmp_files.size(); i++) {
			close(_tmp_files[i]);
		}
	}


	/**
	 * Return the number of elements
	 *
	 * @return the number of elements
	 */
	size_t size() {
		return _size;
	}


	/**
	 * Insert an element
	 *
	 * @param element the element
	 */
	ll_external_sort& operator<< (const T& element) {

		if (_buffer_size >= _buffer_capacity) {
#ifdef LL_XS_DEBUG_PERFORMANCE
			fprintf(stderr, "ll_external_sort::operator<<: buffer >= capacity "
					"(%lu >= %lu)\n", _buffer_size, _buffer_capacity);
#endif
			T* b = sort_buffer();
			_tmp_files.push_back(write_buffer(b, _buffer_size));
			_buffer_size = 0;
			if (b != _buffer) free(b);
		}

		_buffer[_buffer_size++] = element;
		return *this;
	}


	/**
	 * Sort
	 */
	void sort() {

		if (_tmp_files.size() == 0) {
			T* b = sort_buffer();
			if (b != _buffer) free(_buffer);
			_buffer = (T*) realloc(b, sizeof(T) * _buffer_size);	// shrink
		}
		else {
			T* b = sort_buffer();
			_tmp_files.push_back(write_buffer(b, _buffer_size));
			_buffer_size = 0;
			if (b != _buffer) free(b);
		}


#ifdef LL_XS_HIERARCHICAL_MERGE

		size_t block_size = _tmp_buffer_capacity;
		size_t block_bytes = sizeof(T) * block_size;
		T* blockA = (T*) malloc(block_bytes);
		T* blockB = (T*) malloc(block_bytes);
		T* outBuf = (T*) malloc(block_bytes * 2);

		while (_tmp_files.size() > 1) {
			std::vector<int> new_tmp_files;
			_phase++;

			for (size_t i = 0; i + 1 < _tmp_files.size(); i += 2) {
				ssize_t rA, rB, r;

				size_t iA = 1;
				size_t iB = 1;
				size_t nA = 1;
				size_t nB = 1;

				size_t w = 0;
				int f = temporary_file();

				size_t num_read = 0;
				size_t num_written = 0;

				while (nA > 0 && nB > 0) {

					// If ran out of A and there is more left, load more

					if (iA >= nA && nA > 0) {
						rA = read(_tmp_files[i], blockA, block_bytes);
						if (rA < 0) {
							perror("read");
							LL_E_PRINT("read failed\n");
							abort();
						}
						iA = 0;
						nA = rA / sizeof(T);
						num_read += nA;
					}


					// If ran out of B and there is more left, load more

					if (iB >= nB && nB > 0) {
						rB = read(_tmp_files[i+1], blockB, block_bytes);
						if (rB < 0) {
							perror("read");
							LL_E_PRINT("read failed\n");
							abort();
						}
						iB = 0;
						nB = rB / sizeof(T);
						num_read += nB;
					}


					// Merge loaded blocks A and B until we run out of
					// at least one of them, or until the data needs to
					// be written

					while (iA < nA && iB < nB && w < block_size) {

						if (_comparator(blockA[iA], blockB[iB])) {
							outBuf[w++] = blockA[iA++];
						}
						else {
							outBuf[w++] = blockB[iB++];
						}
					}


					// Write the data if we the buffer is full

					if (w >= block_size) {
						r = write(f, outBuf, sizeof(T) * w);
						if (r < sizeof(T) * w) {
							perror("write");
							LL_E_PRINT("write failed: returned %ld; "
									"expected %ld\n", r, sizeof(T) * w);
							abort();
						}
						num_written += w;
						w = 0;
					}
				}


				// Finish

				int copy_from = 0;

				if (nA > 0) {
					assert(nA >= iA);
					if (nA > iA) {
						memcpy(&outBuf[w], &blockA[iA], sizeof(T)*(nA-iA));
						w += nA - iA;
					}
					copy_from = _tmp_files[i];
				}

				if (nB > 0) {
					assert(copy_from == 0);
					assert(nB >= iB);
					if (nB > iB) {
						memcpy(&outBuf[w], &blockB[iB], sizeof(T)*(nB-iB));
						w += nB - iB;
					}
					copy_from = _tmp_files[i+1];
				}

				if (w > 0) {
					r = write(f, outBuf, sizeof(T) * w);
					if (r < sizeof(T) * w) {
						perror("write");
						LL_E_PRINT("write failed: returned %ld; "
								"expected %ld\n", r, sizeof(T) * w);
						abort();
					}
					num_written += w;
				}

				if (copy_from != 0) {
					while ((rA = read(copy_from, outBuf, block_bytes)) > 0) {
						r = write(f, outBuf, rA);
						if (r < rA) {
							perror("write");
							LL_E_PRINT("write failed: returned %ld; "
									"expected %ld\n", r, rA);
							abort();
						}
						num_read += rA / sizeof(T);
						num_written += rA / sizeof(T);
					}
					if (rA < 0) {
						perror("read");
						LL_E_PRINT("read failed\n");
						abort();
					}
				}

				close(_tmp_files[i]);
				close(_tmp_files[i+1]);

				off_t t = lseek(f, 0, SEEK_SET);
				if (t == (off_t) -1) {
					perror("lseek");
					LL_E_PRINT("lseek failed\n");
					abort();
				}
				new_tmp_files.push_back(f);
			}

			if ((_tmp_files.size() & 1) == 1) {
				new_tmp_files.push_back(_tmp_files[_tmp_files.size() - 1]);
			}

			_tmp_files.clear();
			for (size_t i = 0; i < new_tmp_files.size(); i++)
				_tmp_files.push_back(new_tmp_files[i]);
			new_tmp_files.clear();
		}

		free(outBuf);
		free(blockB);
		free(blockA);

		if (_tmp_files.size() >= 1) {
			free(_buffer);
			_buffer_capacity = _tmp_buffer_capacity;
			_buffer = (T*) malloc(sizeof(T) * _buffer_capacity);
		}

#else

		if (_tmp_files.size() >= 1) {

			// Shrink the buffer

			free(_buffer);
			_buffer_capacity = _tmp_buffer_capacity;
			_buffer = (T*) malloc(sizeof(T) * _buffer_capacity);


			// Start the cursor for each file

			for (size_t i = 0; i < _tmp_files.size(); i++) {

				T* block = (T*) malloc(sizeof(T) * _tmp_buffer_capacity);

				size_t r = read(_tmp_files[i], block,
						sizeof(T) * _tmp_buffer_capacity);
				if (r < 0) {
					perror("read");
					LL_E_PRINT("read failed\n");
					abort();
				}

				_tmp_buffers.push_back(block);
				_tmp_buffer_sizes.push_back(r / sizeof(T));
				_tmp_buffer_index.push_back(0);
			}
		}
#endif
	}


	/**
	 * Clear, assuming that sort() has not yet been called. THe behavior is
	 * undefined if it was already called.
	 */
	void clear() {

#ifndef LL_XS_HIERARCHICAL_MERGE
		for (size_t i = 0; i < _tmp_buffers.size(); i++) {
			free(_tmp_buffers[i]);
		}
		_tmp_buffers.clear();
#endif

		for (size_t i = 0; i < _tmp_files.size(); i++) {
			close(_tmp_files[i]);
		}
		_tmp_files.clear();

		_buffer_size = 0;
		_size = 0;
		_done = false;
		_phase = 0;
	}


	/**
	 * Rewind an already-sorted stream
	 */
	void rewind_sorted() {

		_done = false;

#ifdef LL_XS_HIERARCHICAL_MERGE
		LL_NOT_IMPLEMENTED;
#else

		_tmp_buffer_sizes.clear();
		_tmp_buffer_index.clear();

		for (size_t i = 0; i < _tmp_files.size(); i++) {

			int f = _tmp_files[i];
			T* block = _tmp_buffers[i];

			off_t r = lseek(f, 0, SEEK_SET);
			if (r == (off_t) -1) {
				perror("lseek");
				LL_E_PRINT("lseek failed\n");
				abort();
			}

			size_t r2 = read(f, block, sizeof(T) * _tmp_buffer_capacity);
			if (r2 < 0) {
				perror("read");
				LL_E_PRINT("read failed\n");
				abort();
			}

			_tmp_buffer_sizes.push_back(r2 / sizeof(T));
			_tmp_buffer_index.push_back(0);
		}
#endif
	}


	/**
	 * Get the next block of elements
	 *
	 * @param out 
	 * @return true if okay, false if we reached EOF
	 */
	bool next_block(T** p_buffer, size_t* p_size) {

		if (_done) return false;

		if (_tmp_files.size() == 1) {
			int f = _tmp_files[0];
			ssize_t r = read(f, _buffer, sizeof(T) * _buffer_capacity);
			if (r < 0) {
				perror("read");
				LL_E_PRINT("read failed\n");
				abort();
			}
			_buffer_size = r / sizeof(T);

			if (r == 0) _done = true;
		}
		else if (_tmp_files.size() > 1) {

#ifndef LL_XS_HIERARCHICAL_MERGE

			_buffer_size = 0;
			for (size_t w = 0; w < _buffer_capacity; w++) {
				T* v = NULL;
				size_t tx = 0;
				for (size_t t = 0; t < _tmp_buffers.size(); t++) {
					if (_tmp_buffer_sizes[t] <= 0) continue;
					if (_tmp_buffer_index[t] >= _tmp_buffer_sizes[t]) {
						size_t r = read(_tmp_files[t], _tmp_buffers[t],
								sizeof(T) * _tmp_buffer_capacity);
						if (r < 0) {
							perror("read");
							LL_E_PRINT("read failed\n");
							abort();
						}
						_tmp_buffer_index[t] = 0;
						_tmp_buffer_sizes[t] = r / sizeof(T);
						if (_tmp_buffer_sizes[t] <= 0) continue;
					}
					T* vx = &_tmp_buffers[t][_tmp_buffer_index[t]];
					if (v == NULL || _comparator(*vx, *v)) {
						v = vx;
						tx = t;
					}
				}
				_tmp_buffer_index[tx]++;

				if (v == NULL) break;
				_buffer[_buffer_size++] = *v;
			}

			if (_buffer_size == 0) _done = true;
#endif

		}
		else {
			_done = true;
		}

		*p_buffer = _buffer;
		*p_size = _buffer_size;

		return true;
	}


private:


	/**
	 * Sort the buffer. This will modify the contents of the _buffer, but it
	 * will not make it entirely sorted by default.
	 *
	 * @return the sorted buffer; may or may not be equal to the _buffer
	 */
	T* sort_buffer() {

#ifdef LL_XS_DEBUG_PERFORMANCE
		double t_start = __get_time_ms();
#endif

#ifndef LL_XS_MULTICORE_SORT

		std::sort(_buffer, _buffer + _buffer_size, _comparator);

#	ifdef LL_XS_DEBUG_PERFORMANCE
		double t = __get_time_ms() - t_start;
		fprintf(stderr, "ll_external_sort::sort_buffer: %0.3lf ms\n", t);
#	endif

		return _buffer;

#else

		if (_buffer_size < 256*1024) {	// Need to find a good magic number!
			std::sort(_buffer, _buffer + _buffer_size, _comparator);
#	ifdef LL_XS_DEBUG_PERFORMANCE
			double t = __get_time_ms() - t_start;
			fprintf(stderr, "ll_external_sort::sort_buffer: %0.3lf ms\n", t);
#	endif
			return _buffer;
		}

		size_t n = omp_get_max_threads();

		size_t from[n], to[n];
		for (size_t t = 0; t < n; t++) {
			from[t] = t * _buffer_size / n;
			to[t]   = t+1 == n ? _buffer_size : (t+1) * _buffer_size / n;
		}


		// Sort partial buffers

#		pragma omp parallel
		{
			size_t t = omp_get_thread_num();
			std::sort(_buffer + from[t],
					  _buffer + to[t],
					  _comparator);
		}

		if (n == 1) return _buffer;


		// Parallel merge:
		//   1. Divide the first buffer evenly between n threads
		//   2. Find the corresponding points in the other n-1 buffers
		//   3. Merge the n buffers in parallel

		T* r = (T*) malloc(sizeof(T) * _buffer_size);
		assert(r != NULL);

		size_t merge_from[n /* buffer */][n /* thread */], merge_to[n][n];
		for (size_t t = 0; t < n; t++) {
			merge_from[0][t] = t * to[0] / n;
			merge_to[0][t]   = t+1 == n ? to[0] : (t+1) * to[0] / n;
		}

		for (size_t i = 1; i < n; i++) {
			for (size_t t = 0; t < n-1; t++) {
				merge_to[i][t] = find_first_greater_than(_buffer + from[i],
						to[i] - from[i], _buffer[merge_to[0][t]-1]) + from[i];
			}
			merge_to[i][n-1] = to[i];
			merge_from[i][0] = from[i];
			for (size_t t = 1; t < n; t++) {
				merge_from[i][t] = merge_to[i][t-1];
			}
		}
		
		/*for (size_t i = 0; i < n; i++) {
			fprintf(stderr, "\nSorted buffer #%lu: %lu -- %lu (len = %lu)\n",
					i, from[i], to[i], to[i] - from[i]);
			for (size_t t = 0; t < n; t++) {
				fprintf(stderr, "  Part %lu: %lu -- %lu (len = %lu)\n",
						t, merge_from[i][t], merge_to[i][t],
						merge_to[i][t] - merge_from[i][t]);
			}
		}*/

		size_t write_index[n+1];
		write_index[0] = 0;
		for (size_t t = 1; t < n+1; t++) {
			size_t x = 0;
			for (size_t i = 0; i < n; i++) {
				x += merge_to[i][t-1] - merge_from[i][t-1];
			}
			write_index[t] = write_index[t-1] + x;
		}
		assert(write_index[n] == _buffer_size);

#		pragma omp parallel
		{
			int t = omp_get_thread_num();
			size_t index[n], end[n];

			for (size_t i = 0; i < n; i++) {
				index[i] = merge_from[i][t];
				end[i] = merge_to[i][t];
			}

			for (size_t w = write_index[t]; w < write_index[t+1]; w++) {
				T* v = NULL;
				size_t tx = 0;
				for (size_t i = 0; i < n; i++) {
					if (index[i] < end[i]) {
						if (v == NULL || _comparator(_buffer[index[i]], *v)) {
							v = &_buffer[index[i]];
							tx = i;
						}
					}
				}
				index[tx]++;

				r[w] = *v;
			}
		}

#	ifdef LL_XS_DEBUG_PERFORMANCE
		double t = __get_time_ms() - t_start;
		fprintf(stderr, "ll_external_sort::sort_buffer: %0.3lf ms\n", t);
#	endif

		return r;
#endif
	}


	/**
	 * Do a binary search in the buffer and return the first position that
	 * is greater than the given value
	 *
	 * @param buffer the buffer
	 * @param length the buffer length
	 * @param key the value to find 
	 * @return the position
	 */
	size_t find_first_greater_than(const T* buffer, size_t length,
			const T& key) {

		if (length == 0) {
			return 0;
		}

		ssize_t min = 0;
		ssize_t max = length - 1;
		ssize_t mid = 0;

		while (max >= min) {
			
			mid = min + (max - min) / 2;

			if (_comparator(buffer[mid], key)) {
				min = mid + 1;
			}
			else if (_comparator(key, buffer[mid])) {
				max = mid - 1;
			}
			else {
				while (++mid < (ssize_t) length) {
					if (_comparator(key, buffer[mid])) break;
				}
				return mid;
			}
		}

		if (_comparator(buffer[mid], key)) mid++;
		return mid;
	}


	/**
	 * Create and open a temporary file
	 *
	 * @return the file descriptor
	 */
	int temporary_file() {

		const char* dir = _tmp_dirs[_phase % _tmp_dirs.size()].c_str();
		char n[strlen(dir) + 32];

		sprintf(n, "%stmpXXXXXX", dir);
		int f = mkstemp(n);

		if (f <= 0) {
			perror("open");
			LL_E_PRINT("open failed\n");
			assert(f != 0);
			abort();
		}

		unlink(n);
		return f;
	}


	/**
	 * Write the buffer to disk
	 *
	 * @param buffer the buffer
	 * @param size the size
	 * @return file the file descriptor
	 */
	int write_buffer(T* buffer, size_t size) {

		int f = temporary_file();

		size_t t = size * sizeof(T);
		while (t > 0) {
			size_t s = t;
			if (s > sizeof(T) * 1048576ul) s = sizeof(T) * 1048576ul;
			t -= s;

			ssize_t r = write(f, buffer, s);
			if (r < (ssize_t) s) {
				perror("write");
				LL_E_PRINT("write failed; returned %ld\n", r);
				abort();
			}

			buffer += s / sizeof(T);
		}

		off_t r = lseek(f, 0, SEEK_SET);
		if (r == (off_t) -1) {
			perror("lseek");
			LL_E_PRINT("lseek failed\n");
			abort();
		}

		return f;
	}
};

#endif

