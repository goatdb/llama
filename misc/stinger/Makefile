LLAMA=../../llama
STINGER=$(HOME)/Projects/3rd-party/stinger-r633

CC         := g++
CFLAGS     := -Wall -fopenmp -std=gnu++11 -DLL_MEMORY_ONLY -I. \
	-I$(LLAMA)/include -I$(STINGER)/include -Wno-unused-function \
	-Wno-unused-variable -Wno-sign-compare
LFLAGS     := -L$(STINGER) -lstinger -lm -lrt

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

.PHONY: all clean debug

all: $(TARGETS) 

debug: $(patsubst %,%_debug,$(TARGETS))

clean:
	rm -f $(TARGETS)

$(DIR)/%: %.cc Makefile
	@echo CC $@ >&2
	$(CC) $(CFLAGS) -o $@ $< $(LFLAGS)

$(DIR)/%_debug: %.cc Makefile
	@echo CC $@ >&2
	$(CC) $(CFLAGS_D) -o $@ $< $(LFLAGS)

