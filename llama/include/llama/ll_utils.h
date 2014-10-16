/*
 * ll_utils.h
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
 * Parts of this file were adapted from Green-Marl, which includes the
 * following notice:
 *
 * Copyright (c) 2011-2012 Stanford University, unless otherwise specified.
 * All rights reserved.
 *
 * This software was developed by the Pervasive Parallelism Laboratory of
 * Stanford University, California, USA.
 *
 * Permission to use, copy, modify, and distribute this software in source or
 * binary form for any purpose with or without fee is hereby granted, provided
 * that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of Stanford University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */


#ifndef LL_UTILS_H_
#define LL_UTILS_H_

#include "llama/ll_common.h"

#include <sys/time.h>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unistd.h>
#include <vector>


//==========================================================================//
// Macro-programming                                                        //
//==========================================================================//

/**
 * Temporary variable that does not create any naming conflicts to be used
 * within macros
 *
 * @param p the variable prefix
 */
#define ll_tmp_var__(p,l)	_ ## p ## _ ## l
#define ll_tmp_var_(p,l)	ll_tmp_var__(p,l)
#define ll_tmp_var(p)		ll_tmp_var_(p, __LINE__)


/**
 * Begin the in-macro "with" block
 */
#define ll_tmp_with_begin()	if (bool ll_tmp_var(b) = true)


/**
 * A "with" construct for chaining use in macros
 *
 * @param x the variable declaration (and assignment)
 */
#define ll_tmp_with(x) 		for (x; ll_tmp_var(b); )


/**
 * End the in-macro "with" block
 */
#define ll_tmp_with_end()	ll_tmp_var(b) = false


/**
 * Run the following block of code with the following variable declaration
 *
 * @param x the variable declaration (and assignment)
 */
#define ll_with(x) \
	if (bool ll_tmp_var(w) = true) \
		for (x; ll_tmp_var(w); ll_tmp_var(w) = false)



//==========================================================================//
// Helpers                                                                  //
//==========================================================================//

/**
 * Compiler fence
 */
#define __COMPILER_FENCE	asm volatile("":::"memory");


/**
 * Get the time in ms
 *
 * @return the current time in ms
 */
inline double ll_get_time_ms() {
	struct timeval TV;
	gettimeofday(&TV, NULL);
	return TV.tv_sec*1000 + TV.tv_usec*0.001;
}


/**
 * Convert a timeval to ms
 *
 * @param t the timeval
 * @return the time in ms
 */
inline double ll_timeval_to_ms(struct timeval& t) {
	return t.tv_sec*1000 + t.tv_usec*0.001;
}


/**
 * Get a sum
 *
 * @param v the vector
 * @return the sum
 */
template<typename T>
inline T ll_sum(std::vector<T>& v) {
	T sum = 0;
	for (size_t i = 0; i < v.size(); i++) sum += v[i];
	return sum;
}


/**
 * Get a mean
 *
 * @param v the vector
 * @return the mean
 */
template<typename T>
inline double ll_mean(std::vector<T>& v) {
	double sum = 0;
	for (size_t i = 0; i < v.size(); i++) sum += v[i];
	return v.empty() ? 0 : sum / (double) v.size();
}


/**
 * Compute the standard deviation
 *
 * @param v the vector
 * @return the standard deviation
 */
template<typename T>
inline double ll_stdev(std::vector<T>& v) {
	if (v.empty()) return 0;
	double mean = ll_mean(v);
	double x = 0;
	for (size_t i = 0; i < v.size(); i++) {
		x += (v[i]-mean) * (v[i]-mean);
	}
	return std::sqrt(x / v.size());
}


/**
 * Get a 95% confidence
 *
 * @param v the vector
 * @return the confidence
 */
template<typename T>
inline double ll_c95(std::vector<T>& v) {
	if (v.empty()) return 0;
	double stdev = ll_stdev(v);
	return /* Z_(0.96/2) */ 1.96 * stdev / std::sqrt((double) v.size());
}


/**
 * Get the minimum
 *
 * @param v the vector
 * @return the minimum
 */
template<typename T>
inline T ll_min(std::vector<T>& v) {
	if (v.empty()) return 0;
	T m = v[0];
	for (size_t i = 1; i < v.size(); i++) {
		if (v[i] < m) m = v[i];
	}
	return m;
}


/**
 * Get the maximum
 *
 * @param v the vector
 * @return the maximum
 */
template<typename T>
inline T ll_max(std::vector<T>& v) {
	if (v.empty()) return 0;
	T m = v[0];
	for (size_t i = 1; i < v.size(); i++) {
		if (v[i] > m) m = v[i];
	}
	return m;
}


/**
 * Get the file extension
 *
 * @param file_name the file name
 * @return the file extension, or an empty string if none
 */
inline const char* ll_file_extension(const char* file_name) {

	const char* p_dot   = strrchr(file_name, '.');
	const char* p_slash = strrchr(file_name, '/');

	if (p_dot == NULL) return "";
	if ((unsigned long) p_slash > (unsigned long) p_dot) return "";
	if ((unsigned long) (p_slash + 1) == (unsigned long) p_dot) return "";

	return p_dot + 1;
}


/**
 * Extract the class name from the pretty function string
 *
 * @param prettyFunction the pretty function string
 * @return the class name
 */
inline std::string ll_classname(const std::string& prettyFunction) {

	size_t colons = prettyFunction.find("::");
	if (colons == std::string::npos) return "";

	std::string s = prettyFunction;
	for (;;) {
		size_t b = s.find("<");
		if (b == std::string::npos) break;
		size_t n = 1;
		for (size_t i = b+1; i < s.length(); i++) {
			if (s[i] == '<') n++;
			if (s[i] == '>') {
				n--;
				if (n == 0) {
					s = s.substr(0, b) + s.substr(i+1);
					break;
				}
			}
		}
	}

	colons = s.find("::");
	if (colons == std::string::npos) return "";
	s = s.substr(0, colons);

	size_t b = s.find(" ");
	if (b == std::string::npos) return s;
	return s.substr(b + 1);
}


/**
 * A 64-bit random positive integer
 *
 * @return the random number
 */
inline int64_t ll_rand64_positive() {
	int64_t r = (rand() & 0x7fff);
	r = (r << 16) ^ (rand() & 0xffff);
	r = (r << 16) ^ (rand() & 0xffff);
	r = (r << 16) ^ (rand() & 0xffff);
	return r;
}


/**
 * A 64-bit random positive integer -- reentrant version
 *
 * @param seedp the pointer to the internal state
 * @return the random number
 */
inline int64_t ll_rand64_positive_r(unsigned* seedp) {
	int64_t r = (rand_r(seedp) & 0x7fff);
	r = (r << 16) ^ (rand_r(seedp) & 0xffff);
	r = (r << 16) ^ (rand_r(seedp) & 0xffff);
	r = (r << 16) ^ (rand_r(seedp) & 0xffff);
	return r;
}



//==========================================================================//
// Debugging and output                                                     //
//==========================================================================//

//#define D_DEBUG_NODE			0

#define LL_IS_STDERR_TTY (isatty(fileno(stderr)))

#if (defined(_DEBUG) || defined(D_DEBUG_NODE)) && defined(D_EXTRA)
#	define LL_XD_PRINT(format, ...) { \
		fprintf(stderr, "%s[DEBUG] %s::%s %s" format, \
				LL_IS_STDERR_TTY ? LL_AC_CYAN : "", \
				ll_classname(__PRETTY_FUNCTION__).c_str(), \
				__FUNCTION__, \
				LL_IS_STDERR_TTY ? LL_AC_RESET : "", \
				## __VA_ARGS__); }
#else
#	define LL_XD_PRINT(format, ...)
#endif

#if defined(_DEBUG) || defined(D_DEBUG_NODE)
#	define LL_D_PRINT(format, ...) { \
		fprintf(stderr, "%s[DEBUG] %s::%s %s" format, \
				LL_IS_STDERR_TTY ? LL_AC_CYAN : "", \
				ll_classname(__PRETTY_FUNCTION__).c_str(), \
				__FUNCTION__, \
				LL_IS_STDERR_TTY ? LL_AC_RESET : "", \
				## __VA_ARGS__); }
#else
#	define LL_D_PRINT(format, ...)
#endif

#ifdef D_DEBUG_NODE
#	define LL_D_NODE_PRINT(node, format, ...) \
		if ((node) == D_DEBUG_NODE) { LL_D_PRINT("%s[node %ld]%s " format, \
				LL_IS_STDERR_TTY ? LL_AC_CYAN : "", \
				(long) (node), \
				LL_IS_STDERR_TTY ? LL_AC_RESET : "", \
				## __VA_ARGS__); }
#	define LL_D_NODE2_PRINT(node1, node2, format, ...) { \
		LL_D_NODE_PRINT(node1, format, __VA_ARGS__) \
		else LL_D_NODE_PRINT(node2, format, __VA_ARGS__); }
#else
#	define LL_D_NODE_PRINT(node, format, ...)
#	define LL_D_NODE2_PRINT(node1, node2, format, ...)
#endif

#define LL_E_PRINT(format, ...) { \
	fprintf(stderr, "%s[ERROR] %s::%s %s" format, \
			LL_IS_STDERR_TTY ? LL_AC_RED : "", \
			ll_classname(__PRETTY_FUNCTION__).c_str(), \
			__FUNCTION__, \
			LL_IS_STDERR_TTY ? LL_AC_RESET : "", \
			## __VA_ARGS__); }

#define LL_W_PRINT(format, ...) { \
	fprintf(stderr, "%s[WARN ] %s::%s %s" format, \
			LL_IS_STDERR_TTY ? LL_AC_YELLOW : "", \
			ll_classname(__PRETTY_FUNCTION__).c_str(), \
			__FUNCTION__, \
			LL_IS_STDERR_TTY ? LL_AC_RESET : "", \
			## __VA_ARGS__); }

#define LL_I_PRINT(format, ...) { \
	fprintf(stderr, "%s[INFO ] %s::%s %s" format, \
			LL_IS_STDERR_TTY ? LL_AC_BLUE : "", \
			ll_classname(__PRETTY_FUNCTION__).c_str(), \
			__FUNCTION__, \
			LL_IS_STDERR_TTY ? LL_AC_RESET : "", \
			## __VA_ARGS__); }

#define LL_NOT_IMPLEMENTED { \
	LL_E_PRINT("Not implemented: %s:%d\n", __FILE__, __LINE__); \
	abort(); \
}



//==========================================================================//
// Edge Table Helpers                                                       //
//==========================================================================//

#define __prefix_new_(x)			new_##x
#define __prefix_delete_(x)			delete_##x
#define __prefix_new(x)				__prefix_new_(x)
#define __prefix_delete(x)			__prefix_delete_(x)

#define NEW_LL_ET					__prefix_new(LL_ET)
#define DELETE_LL_ET				__prefix_delete(LL_ET)



//==========================================================================//
// Bitmaps                                                                  //
//==========================================================================//

// From: Green-Marl/apps/output_cpp/gm_graph/src/gm_bitmap.cc

inline unsigned _ll_get_bit(unsigned char* Bit, int n) {
    int bit_pos = n / 8;
    int bit_loc = n % 8;
    unsigned val = (Bit[bit_pos] >> bit_loc) & 0x01;
    return val;
}

inline void _ll_set_bit(unsigned char* BitMap, int n) {
    int bit_pos = n / 8;
    int bit_loc = n % 8;
    unsigned char or_val = 0x1 << bit_loc;
    unsigned char org_val = BitMap[bit_pos];
    unsigned char new_val = or_val | org_val;
    BitMap[bit_pos] = new_val;
}

// true if I'm the one who set the bit
inline bool _ll_set_bit_atomic(unsigned char* BitMap, int n) {
    int bit_pos = n / 8;
    int bit_loc = n % 8;
    unsigned char or_val = 0x1 << bit_loc;
    unsigned char old_val = __sync_fetch_and_or(&BitMap[bit_pos], or_val);
    if (((old_val >> bit_loc) & 0x01) == 0) return true;
    return false;
}

inline void _ll_clear_bit(unsigned char* BitMap, int n) {
    int bit_pos = n / 8;
    int bit_loc = n % 8;
    unsigned char and_val = ~(0x1 << bit_loc);
    unsigned char org_val = BitMap[bit_pos];
    unsigned char new_val = org_val & and_val;
    BitMap[bit_pos] = new_val;
}

inline bool _ll_clear_bit_atomic(unsigned char* BitMap, int n) {
    int bit_pos = n / 8;
    int bit_loc = n % 8;
    unsigned char and_val = ~(0x1 << bit_loc);
    unsigned char old_val = __sync_fetch_and_and(&BitMap[bit_pos], and_val);
    if (((old_val >> bit_loc) & 0x01) == 1) return true; // Am I the one who cleared the bit?
    return false;
}



//==========================================================================//
// Atomics                                                                  //
//==========================================================================//

// Also from Green-Marl

#define _ll_cas_asm(ptr, oldval, newval) \
    ({ \
      __typeof__(ptr) ___p = (ptr); \
      __typeof__(*___p) ___oldval = (oldval); \
      __typeof__(*___p) ___newval = (newval); \
      register unsigned char ___result; \
      register __typeof__(*___p) ___readval; \
      if (sizeof(*___p) == 4) { \
        __asm__ __volatile__ ("lock; cmpxchgl %3,%1; sete %0" \
                              : "=q"(___result), "=m"(*___p), "=a"(___readval) \
                              : "r"(___newval), "m"(*___p), "2"(___oldval) \
                              : "memory"); \
      } else if (sizeof(*___p) == 8) { \
        __asm__ __volatile__ ("lock; cmpxchgq %3,%1; sete %0" \
                              : "=q"(___result), "=m"(*___p), "=a"(___readval) \
                              : "r"(___newval), "m"(*___p), "2"(___oldval) \
                              : "memory"); \
      } else { \
        abort(); \
      } \
      (___result==1); \
    })

static inline bool _ll_atomic_compare_and_swap(int *dest, int old_val,
		int new_val) {
    return __sync_bool_compare_and_swap(dest, old_val, new_val);
}

static inline bool _ll_atomic_compare_and_swap(long *dest, long old_val,
		long new_val) {
    return __sync_bool_compare_and_swap(dest, old_val, new_val);
}

static inline bool _ll_atomic_compare_and_swap(long long* dest, long long old_val,
		long long new_val) {
    return __sync_bool_compare_and_swap(dest, old_val, new_val);
}

static inline bool _ll_atomic_compare_and_swap(float *dest, float old_val,
		float new_val) {
    return _ll_cas_asm(dest, old_val, new_val);
}

static inline bool _ll_atomic_compare_and_swap(double *dest, double old_val,
		double new_val) {
    return _ll_cas_asm(dest, old_val, new_val);
}

template<typename T>
inline void ATOMIC_ADD(T* target, T value) {

    if (value == 0) return;

    T oldValue, newValue;
    do {
        oldValue = *target;
        newValue = oldValue + value;
    } while (_ll_atomic_compare_and_swap((T*) target, oldValue, newValue) == false);
}

inline void ATOMIC_AND(bool* target, bool new_value) {
    // if new value is true, AND operation does not make a change
    // if old target value is false, AND operation does not make a change
    if ((new_value == false) && (*target == true)) *target = false;
}

inline void ATOMIC_OR(bool* target, bool new_value) {
    // if new value is false, OR operation does not make a change
    // if old target value is true, OR operation does not make a change
    if ((new_value == true) && (*target == false)) *target = true;
}


#endif

