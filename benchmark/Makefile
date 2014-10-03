#
# Benchmark for LLAMA
#


#
# Compiler setup
#

CC         := g++
CFLAGS     := -Wall -fopenmp -std=c++0x -I. -I../llama/include
LFLAGS     := 


#
# Task specialization
#

ifdef TASK
	BENCHMARK_TASK_ID := $(shell \
		fgrep -A 100 'g_runnable_things[]' benchmark.cc \
		| grep -E '^\W*{ \"ll_' | awk '{print NR-1, "\t", $$0}' \
		| grep '"${TASK}"' | sed -E 's/^([0-9]+)\W.*$$/\1/')
	CFLAGS := -DBENCHMARK_TASK_ID=${BENCHMARK_TASK_ID} \
		-DBENCHMARK_TASK_STR=${TASK} ${CFLAGS}
endif


#
# Other configuration
#

ifdef ONE_VT
	CFLAGS := -DLL_ONE_VT ${CFLAGS}
endif

ifdef FLAT_VT
	CFLAGS := -DLL_FLAT_VT ${CFLAGS}
endif


#
# Debug
#

ifdef DEBUG_NODE
	CFLAGS := ${CFLAGS} -DD_DEBUG_NODE=${DEBUG_NODE}
endif


#
# Set up the flags
#

ifeq ($(VIMRUNTIME),)
ifneq ($(wildcard /usr/bin/color-g++),)
CC         := color-g++
endif
endif

CFLAGS_D   := -ggdb -O0 -D_DEBUG ${CFLAGS}
CFLAGS     := -g -O3 -DNDEBUG ${CFLAGS}

LFLAGS_D   := ${LFLAGS}
LFLAGS     := ${LFLAGS}
#LFLAGS     := ${LFLAGS} -L/usr/local/lib -ljemalloc

CACHEGRIND := valgrind --tool=cachegrind --branch-sim=yes
MEMCHECK   := valgrind --tool=memcheck


#
# Sources
#

HEADERS := ../llama/include/llama/*.h benchmarks/*.h \
	../llama/include/llama/loaders/*.h tools/*.h

COMMON_DEPENDENCIES := ${HEADERS}


#
# Setup targets
#

T_DIR          := ../bin

ifdef TASK
	T_BASE  := ${T_DIR}/bench-${TASK}
else
	T_BASE  := ${T_DIR}/benchmark
endif

ifdef ONE_VT
	T_BASE  := ${T_BASE}-onevt
endif

ifdef FLAT_VT
	T_BASE  := ${T_BASE}-flatvt
endif

CORE_TARGETS   := ${T_BASE}-memory ${T_BASE}-memory-wd ${T_BASE}-persistent \
                  ${T_BASE}-persistent-wd ${T_BASE}-slcsr ${T_BASE}-streaming
DEBUG_TARGETS  := $(patsubst %,%_debug,${CORE_TARGETS})
COMMON_TARGETS := ${CORE_TARGETS}

.PHONY: all clean debug

.SUFFIXES:



#
# Helper targers
#

all: ${COMMON_TARGETS}

clean:
	@rm -rf ${T_DIR}

debug: ${DEBUG_TARGETS}



#
# Core targets
#

${T_BASE}-memory: benchmark.cc ${COMMON_DEPENDENCIES}
	@echo CC $@ >&2
	@mkdir -p ${T_DIR}
	@${CC} -DLL_MEMORY_ONLY ${CFLAGS} $< ${LFLAGS} -o $@

${T_BASE}-persistent: benchmark.cc ${COMMON_DEPENDENCIES}
	@echo CC $@ >&2
	@mkdir -p ${T_DIR}
	@${CC} -DLL_PERSISTENCE ${CFLAGS} $< ${LFLAGS} -o $@

${T_BASE}-slcsr: benchmark.cc ${COMMON_DEPENDENCIES}
	@echo CC $@ >&2
	@mkdir -p ${T_DIR}
	@${CC} -DLL_SLCSR ${CFLAGS} $< ${LFLAGS} -o $@

${T_BASE}-streaming: benchmark.cc ${COMMON_DEPENDENCIES}
	@echo CC $@ >&2
	@mkdir -p ${T_DIR}
	@${CC} -DLL_STREAMING ${CFLAGS} $< ${LFLAGS} -o $@

${T_BASE}-memory-wd: benchmark.cc ${COMMON_DEPENDENCIES}
	@echo CC $@ >&2
	@mkdir -p ${T_DIR}
	@${CC} -DLL_MEMORY_ONLY -DLL_DELETIONS ${CFLAGS} $< ${LFLAGS} -o $@

${T_BASE}-persistent-wd: benchmark.cc ${COMMON_DEPENDENCIES}
	@echo CC $@ >&2
	@mkdir -p ${T_DIR}
	@${CC} -DLL_PERSISTENCE -DLL_DELETIONS ${CFLAGS} $< ${LFLAGS} -o $@



#
# Core debug targets
#

${T_BASE}-memory_debug: benchmark.cc ${COMMON_DEPENDENCIES}
	@echo CC $@ >&2
	@mkdir -p ${T_DIR}
	@${CC} -DLL_MEMORY_ONLY ${CFLAGS_D} $< ${LFLAGS_D} -o $@

${T_BASE}-persistent_debug: benchmark.cc ${COMMON_DEPENDENCIES}
	@echo CC $@ >&2
	@mkdir -p ${T_DIR}
	@${CC} -DLL_PERSISTENCE ${CFLAGS_D} $< ${LFLAGS_D} -o $@

${T_BASE}-slcsr_debug: benchmark.cc ${COMMON_DEPENDENCIES}
	@echo CC $@ >&2
	@mkdir -p ${T_DIR}
	@${CC} -DLL_SLCSR ${CFLAGS_D} $< ${LFLAGS_D} -o $@

${T_BASE}-streaming_debug: benchmark.cc ${COMMON_DEPENDENCIES}
	@echo CC $@ >&2
	@mkdir -p ${T_DIR}
	@${CC} -DLL_STREAMING ${CFLAGS_D} $< ${LFLAGS_D} -o $@

${T_BASE}-memory-wd_debug: benchmark.cc ${COMMON_DEPENDENCIES}
	@echo CC $@ >&2
	@mkdir -p ${T_DIR}
	@${CC} -DLL_MEMORY_ONLY -DLL_DELETIONS ${CFLAGS_D} $< ${LFLAGS_D} -o $@

${T_BASE}-persistent-wd_debug: benchmark.cc ${COMMON_DEPENDENCIES}
	@echo CC $@ >&2
	@mkdir -p ${T_DIR}
	@${CC} -DLL_PERSISTENCE -DLL_DELETIONS ${CFLAGS_D} $< ${LFLAGS_D} -o $@



#
# Profiling targets
#

${T_BASE}-memory_gprof: benchmark.cc ${T_DIR} ${HEADERS} Makefile
	@echo CC $@ >&2
	@${CC} -DLL_MEMORY_ONLY -pg ${CFLAGS} $< ${LFLAGS} -o $@

${T_BASE}-memory_gperf: benchmark.cc ${T_DIR} ${HEADERS} Makefile
	@echo CC $@ >&2
	@${CC} -DLL_MEMORY_ONLY -DGPERF -DGPERF_CPU ${CFLAGS} $< \
		-lprofiler ${LFLAGS} -o $@



#
# Additional debug benchmark targets
#

${T_BASE}-w-memory_debug: benchmark.cc ${T_DIR} ${HEADERS} Makefile
	@echo CC $@ >&2
	@${CC} -DLL_MEMORY_ONLY -DBENCHMARK_WRITABLE \
		-DLL_DELETIONS ${CFLAGS_D} $< ${LFLAGS_D} -o $@



#
# Additional benchmark targets
#

${T_BASE}-w-memory: benchmark.cc ${T_DIR} ${HEADERS} Makefile
	@echo CC $@ >&2
	@${CC} -DLL_MEMORY_ONLY -DBENCHMARK_WRITABLE \
		-DLL_DELETIONS ${CFLAGS} $< ${LFLAGS} -o $@



#
# Obsolete targets
#

oprofile-setup:
	sudo opcontrol --setup \
		--event=LLC_MISSES:10000:0x41:0:1 \
		--event=BR_MISS_PRED_RETIRED:1000:0:0:1 \
		--no-vmlinux

oprofile-benchmark: ${BENCHMARK_TARGETS}
	@for i in $^; do \
		echo ============================== $$i ==============================; \
		sudo opcontrol --reset; \
		sudo opcontrol --start; \
		echo $$i ${INPUT} >&2; \
		$$i ${INPUT} || exit 1; \
		sudo opcontrol --stop; \
		opreport $$i; \
	done

oprofile-singlethreaded-benchmark: ${BENCHMARK_TARGETS}
	@for i in $^; do \
		echo ============================== $$i ==============================; \
		sudo opcontrol --reset; \
		sudo opcontrol --start; \
		echo $$i -GMNumThreads=1 ${INPUT} >&2; \
		$$i -GMNumThreads=1 ${INPUT} || exit 1; \
		sudo opcontrol --stop; \
		opreport $$i; \
	done

counters: ${T_BASE}_counters
	$< -GMNumThreads=1 ${INPUT}

gprof: ${T_BASE}_gprof
	$< -GMNumThreads=1 ${INPUT}

objdump: ${T_BASE}
	@objdump -d $< \
		| sed -En '/^[0-9a-f]+.._ZN22ll_b_triangle_countingI/,$$p' \
		| sed -n '/^$$/q;p' \
		| sed 's/_ZN22ll_b_triangle_countingI.*runEv/triangle_counting/' \
		| sed 's/_ZN27ll_exp_common_neighbor_iter.*E8get_nextEv/cni::get_next/'

objdump-test: ${T_BASE}
	@objdump -d $< \
		| sed -En '/^[0-9a-f]+.._[A-Z][0-9]+TEST_FUNCTION/,$$p' \
		| sed -n '/^$$/q;p' \
		| sed 's/_Z13TEST_FUNCTION.*RT_/TEST_FUNCTION/'

benchmark_swcow: benchmark.cc ${T_DIR} ${HEADERS} Makefile
	@rm -f $@_*.txt
	@for i in 1 2 3 4 5 6 7 8 9 10 24; do \
		echo ============================== $$i ==============================; \
		${CC} -DLL_DEFAULT_ENTRIES_PER_PAGE_BITS=$$i -DLL_VT=ll_vt_swcow_array \
			${CFLAGS} $< ${LFLAGS} -o ${T_DIR}/benchmark_tmp || exit 1; \
		echo $@ $$i > $@_$$i.txt; \
		date >> $@_$$i.txt; \
		echo >> $@_$$i.txt; \
		${T_DIR}/benchmark_tmp ${INPUT} | tee -a $@_$$i.txt; \
	done

benchmark_vt: benchmark.cc ${T_DIR} ${HEADERS} Makefile
	@rm -f $@_*.txt
	@for i in array cow_array swcow_array; do \
		echo ============================== $$i ==============================; \
		${CC} -DLL_VT=ll_vt_$$i ${CFLAGS} $< ${LFLAGS} -o ${T_DIR}/benchmark_tmp || exit 1; \
		echo $@ $$i > $@_$$i.txt; \
		date >> $@_$$i.txt; \
		echo >> $@_$$i.txt; \
		${T_DIR}/benchmark_tmp ${INPUT} | tee -a $@_$$i.txt; \
	done

