#
# LLAMA Application(s)
#

CC         := g++
CFLAGS     := -Wall -fopenmp -std=c++0x -I. -I../../llama/include
LFLAGS     := 

ifeq ($(VIMRUNTIME),)
ifneq ($(wildcard /usr/bin/color-g++),)
CC         := color-g++
endif
endif

CFLAGS_D   := -ggdb -O0 -D_DEBUG ${CFLAGS}
CFLAGS     := -g -O3 -DNDEBUG ${CFLAGS}

DIR        := ../../bin
SOURCES    := $(wildcard *.cc)
TARGETS    := $(patsubst %.cc,$(DIR)/%,$(SOURCES)) 
TARGETS_D  := $(patsubst %,%_debug,${TARGETS})

.PHONY: all clean debug

all: $(TARGETS)

debug: $(TARGETS_D)

clean:
	rm -f $(TARGETS) $(TARGETS_D)

$(DIR)/%_debug: %.cc *.h
	@echo CC $@ >&2
	$(CC) $(CFLAGS_D) -o $@ $< $(LFLAGS)

$(DIR)/%: %.cc *.h
	@echo CC $@ >&2
	$(CC) $(CFLAGS) -o $@ $< $(LFLAGS)

