#
# LLAMA Tools
#

CC         := g++
CFLAGS     := -Wall -fopenmp -std=c++0x -DLL_PERSISTENCE -I. -I../llama/include
LFLAGS     := 

ifeq ($(VIMRUNTIME),)
ifneq ($(wildcard /usr/bin/color-g++),)
CC         := color-g++
endif
endif

CFLAGS_D   := -ggdb -O0 -D_DEBUG ${CFLAGS}
CFLAGS     := -g -O3 -DNDEBUG ${CFLAGS}

DIR        := ../bin
SOURCES    := $(wildcard *.cc)
TARGETS    := $(patsubst %.cc,$(DIR)/%,$(SOURCES)) 

.PHONY: all clean

all: $(TARGETS) 

clean:
	rm -f $(TARGETS)

$(DIR)/%: %.cc
	@echo CC $@ >&2
	$(CC) $(CFLAGS) -o $@ $< $(LFLAGS)

