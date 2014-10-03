#
# Miscellaneous LLAMA Tools
#

CC         := g++
CFLAGS     := -Wall -std=c++0x -fopenmp -I../llama/include
LFLAGS     := 

SOURCES    := $(wildcard *.cpp)
DIR        := ../bin

TARGETS    := $(patsubst %.cpp,$(DIR)/%,$(SOURCES)) 

ifdef DEBUG
	CFLAGS := -O0 -D_DEBUG -ggdb $(CFLAGS)
else
	CFLAGS := -O3 -DNDEBUG -g $(CFLAGS)
endif

.PHONY: all clean

all: $(TARGETS) 

clean:
	rm -f $(TARGETS)

$(DIR)/%: %.cpp
	@echo CC $@ >&2
	$(CC) $(CFLAGS) -o $@ $< $(LFLAGS)

