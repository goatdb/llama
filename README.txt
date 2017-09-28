  LLAMA
=========


 0. Introduction
-----------------

LLAMA is a graph storage and analysis system that supports mutability and
out-of-memory execution built on top of the compressed sparse row (CSR)
representation. Its goal is to perform comparably to immutable main-memory
analysis systems for graphs that fit in memory and to match or outperform
existing out-of-memory analysis systems for graphs that exceed main memory.

Relevant publications:
  * Peter Macko, Virendra Marathe, Daniel Margo, and Margo Seltzer. "LLAMA:
    Efficient Graph Analytics Using Large Multiversioned Arrays." 31st IEEE
    International Conference on Data Engineering (ICDE '15), Seoul, Korea,
    April 2015.
  * Peter Macko. "LLAMA: A Persistent, Mutable Representation for Graphs."
    PhD Dissertation. Harvard University, Cambridge, MA, January 2015.


 1. Configuring and Building LLAMA
-----------------------------------

Prerequisites:
  - Linux or NetBSD
  - GCC 4.4 or newer (4.8 or newer recommended)
  - gperftools (recommended)

LLAMA comes in four different flavors, which you can choose at the compile-time
by defining an appropriate preprocessor directive:
  * LL_MEMORY_ONLY - an in-memory version of LLAMA
  * LL_PERSISTENCE - a persistent version of LLAMA
  * LL_SLCSR - traditional (single-snapshot) CSR

You can also define the following directives:
  * LL_DELETIONS - enable deletion vector (required to support deletions in the
    write-optimized graph store and also required for multiversioned sliding
    windows)
  * LL_FLAT_VT - a multiversioned flat vertex table (disable COW)
  * LL_ONE_VT - a single shared flat vertex table

The benchmark suite bypasses the write-optimized store by default, but you can
change that by defining:
  * BENCHMARK_WRITABLE - use the write-optimized store in the benchmark 

The Makefile has several defined targets for these configurations; the most
useful targets are:
  * benchmark-memory - uses an in-memory version of LLAMA
  * benchmark-persistent - uses a persistent version of LLAMA 
  * benchmark-slcsr - uses the single-snapshot (single-level) version of LLAMA
  * benchmark-memory-wd - uses the in-memory version with a deletion vector
  * benchmark-persistent-wd - uses the persistent version with a deletion
    vector (currently broken)
  * benchmark-w-memory - in-memory version of LLAMA, use write-optimized store
Most benchmark targets have corresponding %_debug targets useful for debugging
LLAMA, such as benchmark-memory_debug or benchmark-persistent_debug. Typing
"make all" builds benchmark-memory, benchmark-persistent, benchmark-slcsr,
benchmark-memory-wd, and benchmark-persistent-wd.

You can further customize the build by passing additional definitions to the
Makefile. If you use the global Makefile, you can use the targets above, but if
you use benchmark/Makefile directly, you might need to modify the targets. Here
are examples of a few things that you can do:
  * make TASK=pagerank benchmark-memory - compile a PageRank-specific benchmark
    so that you do not need to use the -r argument to specify the task when
    running it; using this compile-time specialization usually results in a
    more optimized (and therefore faster) code
  * make ONE_VT=1 benchmark-memory - enable LL_ONE_VT, a single shared flat
    vertex table
  * make FLAT_VT=1 benchmark-memory - enable LL_FLAT_VT, a multiversioned flat
    vertex table
  * make NO_CONT=1 benchmark-memory - enable LL_NO_CONTINUATIONS, which
    disables explicit adjacency list linking
You can use any other combination of these except combining ONE_VT and FLAT_VT.


 2. Using LLAMA
----------------

If you would like to use LLAMA in your project,
  1. Include llama.h in your code
  2. Compile with -fopenmp -std=gnu++11

Please refer to the following files for examples about how to use LLAMA:
  * tools/llama-load.cc - a stand-alone example that loads a LLAMA database
  * examples/llama-pagerank.cc - a stand-alone example that opens an existing
    LLAMA database and computes a few iterations of PageRank

The code includes Doxygen comments, which documents the functionality,
arguments, and return values of most data functions and describes most data
structures and their fields.

A lot of the streaming functionality is currently hard-coded in benchmark.cc,
and it is in a desperate need for refactoring (see the corresponding issue in
TODO.txt) so that it would be possible to easily build stand-alone streaming
applications. If you would like to do that before that happens, implement your
program as another benchmark in the benchmark suite -- or go ahead and do the
refactoring yourself :) We will be grateful for the contribution!


 3. Modifying LLAMA
--------------------

If you would like to contribute to LLAMA, we would certainly appreciate your
help! There is a long list of TODO items and known bugs in docs/TODO.txt, so
feel free to tackle any of those, or if there is anything else that you would
like to do, please feel free!

All source code of LLAMA is in the header files to facilitate deep inlining,
which is crucial for performance, and to enable the use of templates.

Please refer to docs/FILES.txt for short descriptions of files. In short:
  * benchmark/ - contains the benchmark suite
  * examples/ - contains examples (currently just one PageRank program)
  * llama/ - contains the actual source code of LLAMA
  * tools/ - contains the database loading tool

The files in llama/include/llama roughly break into the following categories:
  * Shared high-level functionality, definitions, and configuration:
      ll_common.h               - Shared definitions
      ll_config.h               - Configuration classes
      ll_database.h             - A high-level database class
  * The read-optimized, multi-snapshot (multi-level) graph store:
      ll_edge_table.h           - The edge table
      ll_mem_array.h            - In-memory arrays, both COW and flat, used for
                                  the vertex table and properties
      ll_mlcsr_graph.h          - The (multi-level CSR) read-only graph that
                                  consists of CSR graph stores, such as in- and
                                  out- edges, and node and edge properties
      ll_mlcsr_helpers.h        - Various multi-level CSR helpers
      ll_mlcsr_iterator.h       - Definitions of edge and node iterators
      ll_mlcsr_properties.h     - Multi-level properties
      ll_mlcsr_sp.h             - The actual multi-level CSR implementation
      ll_page_manager.h         - The reference-counted page manager used by
                                  in-memory vertex tables and properties
      ll_persistent_storage.h   - Almost everything related to persistence,
                                  including persistent COW arrays used for
                                  persistent vertex tables and properties
  * The write-optimized graph store:
      ll_growable_array.h       - A growable array
      ll_writable_array.h       - Arrays used by the write-optimized store
      ll_writable_elements.h    - Writable node and edge objects
      ll_writable_graph.h       - The actual write-optimized store
  * Miscellaneous functionality:
      ll_slcsr.h                - Traditional CSR implementation
  * Miscellaneous utilities:
      ll_lock.h                 - Locks and atomic functions
      ll_bfs_template.h         - A BFS template (adapted from GM)
      ll_dfs_template.h         - A DFS template (adapted from GM)
      ll_external_sort.h        - An external sort routine
      ll_mem_helper.h           - Various types of memory pools
      ll_seq.h                  - The sequence container (from GM)
      ll_utils.h                - Miscellaneous utilities
  * Graph loaders and generators, all inside loaders/ :
      ll_gen_erdosrenyi.h       - Random graph generator
      ll_gen_rmat.h             - R-MAT graph generator
      ll_load_async_writable.h  - Helpers for loading the write-store
      ll_loaders.h              - A collection of all loaders
      ll_load_fgf.h             - FGF loader
      ll_load_net.h             - SNAP edge-list loader
      ll_load_utils.h           - Common loader functionality
      ll_load_xstream1.h        - X-Stream Type 1 edge-list loader

