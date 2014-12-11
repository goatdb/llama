/*
 * ll_common.h
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


#ifndef LL_COMMON_H_
#define LL_COMMON_H_

/*
 * ==================== COMMON CONFIGURATION AND HELPERS  ====================
 *
 * Configure by setting one of the following macros:
 *   LL_MEMORY_ONLY
 *   LL_PERSISTENCE
 *   LL_STREAMING
 *   LL_SLCSR
 *
 * Additional configuration:
 *   LL_DELETIONS
 */



//==========================================================================//
// Common Includes                                                          //
//==========================================================================//

#ifdef GPERF
#include <google/profiler.h>
#endif

#include <sys/mman.h>
#include <cstdint>
#include <errno.h>
#include <omp.h>



//==========================================================================//
// Basic Sanity Checks                                                      //
//==========================================================================//

#ifdef LL_MEMORY_ONLY
#	define __I__LL_MEMORY_ONLY		1
#else
#	define __I__LL_MEMORY_ONLY		0
#endif

#ifdef LL_PERSISTENCE
#	define __I__LL_PERSISTENCE		1
#else
#	define __I__LL_PERSISTENCE		0
#endif

#ifdef LL_STREAMING
#	define __I__LL_STREAMING		1
#else
#	define __I__LL_STREAMING		0
#endif

#ifdef LL_SLCSR
#	define __I__LL_SLCSR			1
#else
#	define __I__LL_SLCSR			0
#endif

#if (__I__LL_MEMORY_ONLY + __I__LL_PERSISTENCE + __I__LL_STREAMING \
		+ __I__LL_SLCSR != 1)
#	error "You must specify one of the four LLAMA configurations"
#endif



//==========================================================================//
// Graph types and constants                                                //
//==========================================================================//

typedef int64_t node_t;
typedef int64_t edge_t;
typedef uint32_t degree_t;

typedef struct {
	node_t tail;
	node_t head;
} node_pair_t;

#define LL_NIL				-1
#define LL_NIL_NODE			((node_t) LL_NIL)
#define LL_NIL_EDGE			((edge_t) LL_NIL)


/**
 * Comparator for node_t
 */
struct ll_node_comparator {
	bool operator() (const node_t& a, const node_t& b) {
		return a < b;
	}
};


/**
 * Comparator for edge_t
 */
struct ll_edge_comparator {
	bool operator() (const edge_t& a, const edge_t& b) {
		return a < b;
	}
};



//==========================================================================//
// Advice constants                                                         //
//==========================================================================//

#define LL_ADV_DONTNEED		MADV_DONTNEED
#define LL_ADV_WILLNEED		MADV_WILLNEED



//==========================================================================//
// Generally useful constants                                               //
//==========================================================================//

/*
 * GCC Version
 */
#define GCC_VERSION (__GNUC__*10000 + __GNUC_MINOR__*100 + __GNUC_PATCHLEVEL__)

/*
 * Color strings
 */
#define LL_AC_RED     		"\x1b[31m"
#define LL_AC_GREEN   		"\x1b[32m"
#define LL_AC_YELLOW  		"\x1b[33m"
#define LL_AC_BLUE    		"\x1b[34m"
#define LL_AC_MAGENTA 		"\x1b[35m"
#define LL_AC_CYAN    		"\x1b[36m"
#define LL_AC_RESET   		"\x1b[0m"



//==========================================================================//
// Types                                                                    //
//==========================================================================//

/*
 * Property types (FGF compatible)
 */
#define LL_T_STRING			0x1
#define LL_T_BOOLEAN		0x10
#define LL_T_INT16			0x11
#define LL_T_INT32			0x12
#define LL_T_INT64			0x13
#define LL_T_FLOAT			0x20
#define LL_T_DOUBLE			0x21


/**
 * Is the type 32-bit integral?
 *
 * @param t the type
 * @return true if it is
 */
inline bool ll_is_type_integral32(short t) {
	return t == LL_T_INT32 || t == LL_T_FLOAT;
}


/**
 * Is the type 64-bit integral?
 *
 * @param t the type
 * @return true if it is
 */
inline bool ll_is_type_integral64(short t) {
	return t == LL_T_INT64 || t == LL_T_DOUBLE;
}


/**
 * Is the type floating point?
 *
 * @param t the type
 * @return true if it is
 */
inline bool ll_is_type_floating_point(short t) {
	return t == LL_T_FLOAT || t == LL_T_DOUBLE;
}



//==========================================================================//
// Main Memory Only Configuration                                           //
//==========================================================================//

#ifdef LL_MEMORY_ONLY
#	define LL_CSR					ll_mlcsr_core
#	define LL_VT					ll_mem_array_swcow
#	define LL_VT_COLLECTION			ll_mem_array_collection
#	define LL_PT					ll_mem_array_swcow
#	define LL_PT_COLLECTION			ll_mem_array_collection
#	define LL_ET					ll_et_array
#endif


//==========================================================================//
// Persistence Configuration                                                //
//==========================================================================//

#ifdef LL_PERSISTENCE
#	define LL_CSR					ll_mlcsr_core
#	define LL_VT					ll_persistent_array_swcow
#	define LL_VT_COLLECTION			ll_persistent_array_collection
#	define LL_PT					ll_persistent_array_swcow
#	define LL_PT_COLLECTION			ll_persistent_array_collection
#	define LL_ET					ll_et_mmaped_array
#endif


//==========================================================================//
// Streaming Configuration                                                  //
//==========================================================================//

#ifdef LL_STREAMING
#	define LL_CSR					ll_mlcsr_core
#	define LL_VT					ll_mem_array_swcow
#	define LL_VT_COLLECTION			ll_mem_array_collection
#	define LL_PT					ll_mem_array_swcow
#	define LL_PT_COLLECTION			ll_mem_array_collection
#	define LL_ET					ll_et_array
#endif

#ifdef LL_STREAMING
#	define LL_MIN_LEVEL
#	define LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES
#	define LL_S_UPDATE_PRECOMPUTED_DEGREES
#	ifndef DEL_PRESET
#		define LL_DELETIONS
#	endif
#endif


//==========================================================================//
// SL-CSR Configuration                                                     //
//==========================================================================//

#ifdef LL_SLCSR
#	define LL_CSR					ll_slcsr
#	define LL_VT					ll_mem_array_flat
#	define LL_VT_COLLECTION			ll_mem_array_collection
#	define LL_PT					ll_mem_array_flat
#	define LL_PT_COLLECTION			ll_mem_array_collection
#	define LL_ET					ll_et_array
#endif

#ifdef LL_SLCSR
#	define FORCE_L0
#endif

#ifdef LL_SLCSR
#	ifdef LL_DELETIONS
#		error "LL_SLCSR and LL_DELETIONS cannot be defined at the same time"
#	endif
#endif


//==========================================================================//
// The Writable Representation                                              //
//==========================================================================//

#define LL_W_VT						ll_w_vt_swcow_array


//==========================================================================//
// Configuration Modifications                                              //
//==========================================================================//

#ifdef LL_ONE_VT
#	define LL_FLAT_VT
#endif

#ifdef LL_FLAT_VT
#	ifdef LL_PERSISTENCE
#		error "Cannot combine LL_FLAT_VT with LL_PERSISTENCE"
#	endif
#	undef LL_VT
#	undef LL_PT
#	define LL_VT					ll_mem_array_flat
#	define LL_PT					ll_mem_array_flat
#endif


//==========================================================================//
// Additional Configuration                                                 //
//==========================================================================//

#define LL_PRECOMPUTED_DEGREE
#define LL_REVERSE_EDGES
//#define LL_SORT_EDGES

#ifndef LL_NO_CONTINUATIONS
#	define LL_MLCSR_CONTINUATIONS
#endif

//#define LL_MIN_LEVEL
//#define LL_MLCSR_LEVEL_ID_WRAP

//#define LL_TX
//#define LL_TIMESTAMPS

//#define FORCE_L0

//#define LL_CHECK_NODE_EXISTS_IN_RO
#define LL_CHECK_NODE_FASTER

//#define ITERATOR_DECL inline
#define ITERATOR_DECL

#define WORD_ACCESS_IS_ALREADY_ATOMIC	/* already true for x86 */

#ifdef LL_STREAMING
#	ifndef LL_ENTRIES_PER_PAGE_BITS
//#		define LL_ENTRIES_PER_PAGE_BITS	5
#	endif
#endif

#ifndef LL_ENTRIES_PER_PAGE_BITS
#	define LL_ENTRIES_PER_PAGE_BITS		9
#endif
#define LL_ENTRIES_PER_PAGE				(1 << LL_ENTRIES_PER_PAGE_BITS)

#define LL_D_STRIPES					256
#define LL_D_STRIPE_BASE_SHIFT			5
#define LL_D_STRIPE(x)					(((x) >> LL_D_STRIPE_BASE_SHIFT) \
											& (LL_D_STRIPES - 1))

#if defined(LL_MLCSR_LEVEL_ID_WRAP) && !defined(LL_MIN_LEVEL)
#	error "LL_MLCSR_LEVEL_ID_WRAP requires LL_MIN_LEVEL"
#endif

#if defined(LL_MLCSR_LEVEL_ID_WRAP) && defined(LL_PERSISTENCE)
#	error "LL_MLCSR_LEVEL_ID_WRAP and LL_PERSISTENCE are not compatible"
#endif


//==========================================================================//
// Adjacency List Helpers                                                   //
//==========================================================================//

// TODO Need separate deletion vector to support efficient deletion of frozen
// edges in the writable layer when LL_COPY_ADJ_LIST_ON_DELETION is enabled

/*
 * Adjacency list length
 */
typedef unsigned length_t;



//==========================================================================//
// Configuration Helpers                                                    //
//==========================================================================//

#ifdef LL_PRECOMPUTED_DEGREE
#define IF_LL_PRECOMPUTED_DEGREE(...) __VA_ARGS__
#else
#define IF_LL_PRECOMPUTED_DEGREE(...)
#endif

#ifdef LL_STREAMING
#define IF_LL_STREAMING(...) __VA_ARGS__
#define IFN_LL_STREAMING(...)
#define IFE_LL_STREAMING(t, e) t
#else
#define IF_LL_STREAMING(...)
#define IFN_LL_STREAMING(...) __VA_ARGS__
#define IFE_LL_STREAMING(t, e) e
#endif

#ifdef LL_PERSISTENCE
#define IF_LL_PERSISTENCE(...) __VA_ARGS__
#define IFE_LL_PERSISTENCE(t, e) t
#else
#define IF_LL_PERSISTENCE(...)
#define IFE_LL_PERSISTENCE(t, e) e
#endif

#ifdef LL_DELETIONS
#define IF_LL_DELETIONS(...) __VA_ARGS__
#define IFE_LL_DELETIONS(t, e) t
#else
#define IF_LL_DELETIONS(...)
#define IFE_LL_DELETIONS(t, e) e
#endif

#ifdef LL_MLCSR_CONTINUATIONS
#define IF_LL_MLCSR_CONTINUATIONS(...) __VA_ARGS__
#define IFE_LL_MLCSR_CONTINUATIONS(t, e) t
#else
#define IF_LL_MLCSR_CONTINUATIONS(...)
#define IFE_LL_MLCSR_CONTINUATIONS(t, e) e
#endif

#ifdef LL_MLCSR_LEVEL_ID_WRAP
#define IF_LL_MLCSR_LEVEL_ID_WRAP(...) __VA_ARGS__
#define IFE_LL_MLCSR_LEVEL_ID_WRAP(t, e) t
#else
#define IF_LL_MLCSR_LEVEL_ID_WRAP(...)
#define IFE_LL_MLCSR_LEVEL_ID_WRAP(t, e) e
#endif

#endif
