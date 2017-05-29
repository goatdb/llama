#
# LLAMA Tools
#

CC         := g++
CFLAGS     := -Wall -fopenmp -std=gnu++11 -DLL_MEMORY_ONLY -I. -I../../llama/include
LFLAGS     := -lreadline

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

.PHONY: all clean

all: $(TARGETS) 

clean:
	rm -f $(TARGETS)

$(DIR)/%: %.cc
	@echo CC $@ >&2
	$(CC) $(CFLAGS) -o $@ $< $(LFLAGS)

$(DIR)/%_debug: %.cc
	@echo CC $@ >&2
	$(CC) $(CFLAGS_D) -o $@ $< $(LFLAGS)

