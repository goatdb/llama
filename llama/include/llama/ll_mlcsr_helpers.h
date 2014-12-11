/*
 * ll_mlcsr_helpers.h
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


#ifndef LL_MLCSR_HELPERS_H_
#define LL_MLCSR_HELPERS_H_

#include <sys/time.h>
#include <sys/resource.h>

#include "llama/ll_writable_elements.h"



//==========================================================================//
// Helpers                                                                  //
//==========================================================================//

/*
 * Sizes in bits
 */

#ifdef LL_NODE32
#	define LL_DATA_TYPE							unsigned
#	define LL_ONE								1u
#	define LL_BITS_TOTAL						32
#	define LL_BITS_LEVEL						4
#else
#	define LL_DATA_TYPE							unsigned long long
#	define LL_ONE								1ull
#	define LL_BITS_TOTAL						64
#	define LL_BITS_LEVEL						26
#endif

#define LL_MASK(bits)							((LL_ONE << (bits)) - 1)
#define LL_BITS_INDEX							(LL_BITS_TOTAL - LL_BITS_LEVEL)


/*
 * Edges
 */

#define LL_MAX_LEVEL						(LL_MASK(LL_BITS_LEVEL) - 2)

#define LL_EDGE_CREATE(level, index)		((index) | (((LL_DATA_TYPE) level) << LL_BITS_INDEX))
#define LL_EDGE_LEVEL(x)					(((LL_DATA_TYPE) (x)) >> LL_BITS_INDEX)
#define LL_EDGE_INDEX(x)					((x) & LL_MASK(LL_BITS_INDEX))
#define LL_EDGE_NEXT_INDEX(x)				((x) + LL_ONE)

#define LL_WRITABLE_LEVEL					(LL_MAX_LEVEL + 1)
#define LL_EDGE_IS_WRITABLE(x)				(LL_EDGE_LEVEL(x) == LL_WRITABLE_LEVEL)


/*
 * Values
 */

#ifdef LL_DELETIONS
#	ifdef LL_NODE32
#		error "LL_NODE32 does not support LL_DELETIONS."
#	endif
#	define LL_CHECK_EXT_DELETION			(LL_MAX_LEVEL + 1)

	// max_visible_level is exclusive

#	define LL_VALUE_CREATE_EXT(payload, \
		max_visible_level)						(((LL_DATA_TYPE) (payload)) \
													| ((LL_DATA_TYPE) (max_visible_level) \
														<< (LL_BITS_TOTAL - LL_BITS_LEVEL)))
#	define LL_VALUE_CREATE(payload)			LL_VALUE_CREATE_EXT(payload, LL_MAX_LEVEL + 2)

#	define LL_VALUE_PAYLOAD(x)				((x) & LL_MASK(LL_BITS_TOTAL - LL_BITS_LEVEL))
#	define LL_VALUE_MAX_LEVEL(x)			((((LL_DATA_TYPE) x) >> (LL_BITS_TOTAL - LL_BITS_LEVEL)) \
													& LL_MASK(LL_BITS_LEVEL))
#	define LL_VALUE_IS_DELETED(x, level)	(LL_VALUE_MAX_LEVEL(x) <= (level))

#else /* ! LL_DELETIONS */

#	define LL_VALUE_CREATE(payload)			((LL_DATA_TYPE) (payload))
#	define LL_VALUE_PAYLOAD(x)				(x)

#endif

inline bool ll_payload_compare(long i, long j) {
	return LL_VALUE_PAYLOAD(i) < LL_VALUE_PAYLOAD(j);
}

inline bool ll_payload_pair_compare(const std::pair<long, long>& i,
		const std::pair<long, long>& j) {

	long ip = LL_VALUE_PAYLOAD(i.first);
	long jp = LL_VALUE_PAYLOAD(j.first);

	if (ip != jp)
		return ip < jp;
	else
		return i.second < j.second;
}



//==========================================================================//
// ll_mlcsr_core__begin_t                                                   //
//==========================================================================//

/**
 * An element in the vertex table
 */
struct ll_mlcsr_core__begin_t {

	edge_t adj_list_start;
	length_t level_length;

#ifdef LL_PRECOMPUTED_DEGREE
	degree_t degree;
#endif
};


/**
 * Compare two vertex table elements
 *
 * @param a the first element
 * @param b the second element
 * @return true if they are equal
 */
inline bool operator== (const ll_mlcsr_core__begin_t& a,
		const ll_mlcsr_core__begin_t& b) {

#ifdef LL_PRECOMPUTED_DEGREE
	return a.adj_list_start == b.adj_list_start
		&& a.level_length == b.level_length
		&& a.degree == b.degree;
#else
	return a.adj_list_start == b.adj_list_start
		&& a.level_length == b.level_length;
#endif
}


/**
 * Compare two vertex table elements
 *
 * @param a the first element
 * @param b the second element
 * @return true if they are not equal
 */
inline bool operator!= (const ll_mlcsr_core__begin_t& a,
		const ll_mlcsr_core__begin_t& b) {
	return !(a == b);
}



//==========================================================================//
// Debugging routines                                                       //
//==========================================================================//

#if 0

static long __d_maxrss_last = 0;
static double __d_t_last = 0;

/**
 * Print MAXRSS and the delta from last
 */
void __d_maxrss(const char* msg = NULL) {

	struct rusage r;
	getrusage(RUSAGE_SELF, &r);
	long m = r.ru_maxrss;

	fprintf(stderr, "[DEBUG] MaxRSS=%7.2lf MB, Delta=%7.2lf MB   %s\n",
			m / 1024.0, (m - __d_maxrss_last) / 1024.0, msg == NULL ? "" : msg);

	__d_maxrss_last = m;
}


/**
 * Timing
 *
 * @param msg the message to print, use NULL to reset
 */
double __d_time(const char* msg) {

	struct timeval TV;
	gettimeofday(&TV, NULL);
	double t = TV.tv_sec*1000 + TV.tv_usec*0.001;

	if (msg != NULL) {
		fprintf(stderr, "[DEBUG] %s: %7.2lf ms\n", msg, t - __d_t_last);
	}

	gettimeofday(&TV, NULL);
	__d_t_last = TV.tv_sec*1000 + TV.tv_usec*0.001;

	return __d_t_last;
}

#endif



//==========================================================================//
// Destructors                                                              //
//==========================================================================//

template <typename T>
void destructor64(const uint64_t& value) {
	if (value != 0) delete (T*) value;
}



//==========================================================================//
// Interface: ll_mlcsr_external_deletions                                   //
//==========================================================================//

/**
 * The data source for external deletions
 */
class ll_mlcsr_external_deletions {

public:

	ll_mlcsr_external_deletions() {}

	virtual ~ll_mlcsr_external_deletions() {}

	virtual bool is_edge_deleted(edge_t edge) = 0;
};



//==========================================================================//
// Other                                                                    //
//==========================================================================//

// Writable vertex table
typedef LL_W_VT<long, 0l, -1l, w_node_allocator_ext<long>,
		w_node_deallocator_ext<long>> ll_w_vt_vertices_t;


#endif

