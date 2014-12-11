#
# LLAMA's top level makefile
#

TARGETS := benchmark examples tools utils

BENCHMARK_CORE_TARGETS := benchmark-memory benchmark-memory-wd \
	benchmark-persistent benchmark-persistent-wd benchmark-slcsr \
	benchmark-streaming benchmark-streaming-weights
BENCHMARK_CORE_DEBUG_TARGETS := $(patsubst %,%_debug,${BENCHMARK_CORE_TARGETS})
BENCHMARK_OTHER_TARGETS := benchmark-w-memory benchmark-w-slcsr benchmark-w-memory-nd \
	benchmark-ss-streaming benchmark-ss-streaming-weights
BENCHMARK_OTHER_DEBUG_TARGETS := $(patsubst %,%_debug,${BENCHMARK_OTHER_TARGETS})
BENCHMARK_TARGETS := ${BENCHMARK_CORE_TARGETS} ${BENCHMARK_CORE_DEBUG_TARGETS}\
	${BENCHMARK_OTHER_TARGETS} ${BENCHMARK_OTHER_DEBUG_TARGETS}

ifdef TASK
	BENCHMARK_BASE  := bench-${TASK}
else
	BENCHMARK_BASE  := benchmark
endif

ifdef ONE_VT
	BENCHMARK_BASE  := ${BENCHMARK_BASE}-onevt
	MFLAGS          := ${MFLAGS} ONE_VT=${ONE_VT}
endif

ifdef FLAT_VT
	BENCHMARK_BASE  := ${BENCHMARK_BASE}-flatvt
	MFLAGS          := ${MFLAGS} FLAT_VT=${FLAT_VT}
endif

ifdef NO_CONT
	BENCHMARK_BASE  := ${BENCHMARK_BASE}-nocont
	MFLAGS          := ${MFLAGS} NO_CONT=${NO_CONT}
endif

MFLAGS := ${MFLAGS} TASK=${TASK} DEBUG_NODE=${DEBUG_NODE}

.PHONY: all clean ${BENCHMARK_TARGETS}

all clean:
	@for t in ${TARGETS}; do \
		${MAKE} ${MFLAGS} -C "$$t" $@ || exit 1; \
	done

${BENCHMARK_TARGETS}:
	@${MAKE} ${MFLAGS} -C benchmark \
		../bin/`echo "$@" | sed "s/benchmark/${BENCHMARK_BASE}/g"`

