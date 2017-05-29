#
# LLAMA Application(s)
#

CC         := g++
CFLAGS     := -Wall -fopenmp -std=gnu++11 -I. -I.. -I../../llama/include
LFLAGS     := 

ifeq ($(VIMRUNTIME),)
ifneq ($(wildcard /usr/bin/color-g++),)
CC         := color-g++
endif
endif

CFLAGS_D   := -ggdb -O0 -D_DEBUG ${CFLAGS}
CFLAGS     := -g -O3 -DNDEBUG ${CFLAGS}

DIR        := ../../bin
SOURCES    := $(wildcard sloth-*.cc)
TARGETS_C  := $(patsubst %.cc,$(DIR)/%,$(SOURCES)) 
TARGETS    := ${TARGETS_C} \
			  $(patsubst %,%_ss,${TARGETS_C})  \
			  $(patsubst %,%_ssd,${TARGETS_C}) \
			  $(patsubst %,%_ssw,${TARGETS_C}) \
			  $(patsubst %,%_d,${TARGETS_C}) \
			  $(patsubst %,%_w,${TARGETS_C})
TARGETS_D  := $(patsubst %,%_debug,${TARGETS})

.PHONY: all clean debug

all: $(TARGETS)

debug: $(TARGETS_D)

clean:
	rm -f $(TARGETS) $(TARGETS_D)


# Debug targets

$(DIR)/%_ss_debug: %.cc *.h ../common/*.h
	@echo CC $@ >&2
	$(CC) $(CFLAGS_D) -DLL_S_SINGLE_SNAPSHOT -o $@ $< $(LFLAGS)

$(DIR)/%_ssd_debug: %.cc *.h ../common/*.h
	@echo CC $@ >&2
	$(CC) $(CFLAGS_D) -DLL_S_SINGLE_SNAPSHOT -DDEDUP -o $@ $< $(LFLAGS)

$(DIR)/%_ssw_debug: %.cc *.h ../common/*.h
	@echo CC $@ >&2
	$(CC) $(CFLAGS_D) -DLL_S_SINGLE_SNAPSHOT \
		-DLL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES -DDEDUP -o $@ $< $(LFLAGS)

$(DIR)/%_d_debug: %.cc *.h ../common/*.h
	@echo CC $@ >&2
	$(CC) $(CFLAGS_D) -DDEDUP -o $@ $< $(LFLAGS)

$(DIR)/%_w_debug: %.cc *.h ../common/*.h
	@echo CC $@ >&2
	$(CC) $(CFLAGS_D) -DLL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES -DDEDUP \
		-o $@ $< $(LFLAGS)

$(DIR)/%_debug: %.cc *.h ../common/*.h
	@echo CC $@ >&2
	$(CC) $(CFLAGS_D) -DDEL_PRESET -o $@ $< $(LFLAGS)


# Real targets

$(DIR)/%_ss: %.cc *.h ../common/*.h
	@echo CC $@ >&2
	$(CC) $(CFLAGS) -DLL_S_SINGLE_SNAPSHOT -o $@ $< $(LFLAGS)

$(DIR)/%_ssd: %.cc *.h ../common/*.h
	@echo CC $@ >&2
	$(CC) $(CFLAGS) -DLL_S_SINGLE_SNAPSHOT -DDEDUP -o $@ $< $(LFLAGS)

$(DIR)/%_ssw: %.cc *.h ../common/*.h
	@echo CC $@ >&2
	$(CC) $(CFLAGS) -DLL_S_SINGLE_SNAPSHOT \
		-DLL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES -DDEDUP -o $@ $< $(LFLAGS)

$(DIR)/%_d: %.cc *.h ../common/*.h
	@echo CC $@ >&2
	$(CC) $(CFLAGS) -DDEDUP -o $@ $< $(LFLAGS)

$(DIR)/%_w: %.cc *.h ../common/*.h
	@echo CC $@ >&2
	$(CC) $(CFLAGS) -DLL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES -DDEDUP \
		-o $@ $< $(LFLAGS)

$(DIR)/%: %.cc *.h ../common/*.h
	@echo CC $@ >&2
	$(CC) $(CFLAGS) -DDEL_PRESET -o $@ $< $(LFLAGS)

