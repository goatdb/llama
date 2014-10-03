#!/bin/bash

#
# run-tests.sh
# LLAMA Graph Analytics
#
# Copyright 2014
#      The President and Fellows of Harvard College.
#
# Copyright 2014
#      Oracle Labs.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
# 3. Neither the name of the University nor the names of its contributors
#    may be used to endorse or promote products derived from this software
#    without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE UNIVERSITY AND CONTRIBUTORS ``AS IS'' AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE UNIVERSITY OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
# OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.
#


#
# The test infrastructure
#

ORG_PWD="`pwd`"
ROOT_TESTS="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_LLAMA="$( cd "$ROOT_TESTS/.." && pwd )"
ROOT_RESULTS=/var/www/llama-tests
ROOT_SCRATCH=/var/www/llama-tests/scratch

CHECKOUT_CLEAN=0
CHECKOUT_SOURCE="$ROOT_LLAMA"

LINK_TO_BUILD_DIR=

DATA_DIRECTORY="$ROOT_TESTS/data"
DATA_FILES="$DATA_DIRECTORY/*"

EXPECTED_DIRECTORY="$ROOT_TESTS/expected"

BENCHMARK_PROGRAMS="benchmark-memory"
BENCHMARK_TASKS="pagerank pagerank_push tc_od__-OD"
BENCHMARK_ARGS="-c 3"


#
# Parse the arguments
#

while getopts ":d:hL:R:" opt; do
	case $opt in
		d)
			mkdir -p "$OPTARG"
			if [ ${PIPESTATUS[0]} != 0 ]; then
				echo "Cannot create the results directory: $OPTARG" >&2
				exit 1
			fi
			if [ $ROOT_RESULTS/scratch == $ROOT_SCRATCH ]; then
				ROOT_SCRATCH="$(cd "$ORG_PWD" && cd "$OPTARG" && pwd)"/scratch
			fi
			ROOT_RESULTS="$(cd "$ORG_PWD" && cd "$OPTARG" && pwd)"
			;;
		h)
			echo "Usage: `basename $0` [OPTIONS]" >&2
			echo >&2
			echo "Options:" >&2
			echo "  -d DIR     Set the results directory" >&2
			echo "  -h         Print this help and exit" >&2
			echo "  -L DIR     Add a directory to be sym-linked to the build root" >&2
			echo "  -R GIT     Set the GIT repository location" >&2
			exit 0
			;;
		L)
			X="$(cd "$ORG_PWD" && cd "$OPTARG" && pwd)"
			LINK_TO_BUILD_DIR="$LINK_TO_BUILD_DIR $X"
			;;
		R)
			CHECKOUT_SOURCE="$OPTARG"
			CHECKOUT_CLEAN=1
			;;
		\?)
			echo "Invalid option: -$OPTARG" >&2
			exit 1
			;;
		:)
			echo "Option -$OPTARG requires an argument." >&2
			exit 1
			;;
	esac
done



#
# Initialize a few more global variables
#

START_TIME=`date +%Y%m%d-%H%M%S`

RESULTS_DIR="$ROOT_RESULTS/$START_TIME"
SCRATCH_DIR="$ROOT_SCRATCH/$START_TIME"
REPORT_FILE="$RESULTS_DIR/report.txt"



#
# Colors and output control
#

if [ -t 1 ] ; then
	
	C_RESET="\e[0m"
	C_BOLD="\e[1m"
	C_RED="\e[91m"
	C_BLUE="\e[94m"
	C_YELLOW="\e[93m"
	C_GREEN="\e[92m"
	
	C_H1=$C_BOLD$C_BLUE
	C_H2=$C_BOLD
	C_ERROR=$C_BOLD$C_RED
	C_WARNING=$C_YELLOW
	C_SUCCESS=$C_BOLD$C_GREEN
fi

save() {
	tee -a $REPORT_FILE
}

heading1() {
	echo -e "${C_H1}$*${C_RESET}"
	echo -e "$*" >> $REPORT_FILE
}

heading2() {
	echo -e "${C_H2}$*${C_RESET}"
	echo -e "$*" >> $REPORT_FILE
}

p() {
	echo -e "$*" | save
}

error() {
	echo -e "${C_ERROR}${C_BOLD}Error:${C_RESET}${C_ERROR} $*${C_RESET}"
	echo -e "Error: $*" >> $REPORT_FILE
}

warning() {
	echo -e "${C_WARNING}${C_BOLD}Warning:${C_RESET}${C_WARNING} $*${C_RESET}"
	echo -e "Warning: $*" >> $REPORT_FILE
}

success() {
	echo -e "${C_SUCCESS}$*${C_RESET}"
	echo -e "$*" >> $REPORT_FILE
}

die() {
	error $*
	$ROOT_TESTS/generate-report.py -d $ROOT_RESULTS \
		|| error "Cannot generate the report"
	exit 1
}



#
# Start
#

mkdir -p "$RESULTS_DIR" \
	|| die "Cannot create the results directory:\n       $RESULTS_DIR"

heading1 LLAMA Test Suite
p

mkdir -p "$SCRATCH_DIR" \
	|| die "Cannot create the scratch directory:\n       $SCRATCH_DIR"

if [ x$CHECKOUT_CLEAN = x1 ]; then
	GIT_REVISION=`git ls-remote "$CHECKOUT_SOURCE" HEAD | head -n 1 \
		| awk '{print $1}'`
	if [ ${PIPESTATUS[0]} != 0 ]; then
		die "Cannot determine the GIT revision number:\n       $CHECKOUT_SOURCE"
	fi
else
	cd "$ROOT_LLAMA"
	GIT_REVISION="`git rev-parse HEAD`"
	if [ ${PIPESTATUS[0]} != 0 ]; then
		die "Cannot determine the GIT revision number:\n       $ROOT_LLAMA"
	fi
fi

cd "$ORG_PWD"
cd ..
p "Date             :" `date`
p "Host             :" `hostname`
p "Platform         :" `uname -srio`
p "Arguments        :" $BENCHMARK_ARGS
p "Benchmark GIT    :" $GIT_REVISION
p "Test Suite GIT   :" `cd $ROOT_LLAMA && git rev-parse HEAD`
p "Results directory: $RESULTS_DIR"
cd $ROOT_TESTS


#
# Build
#

p
heading2 "Setting up LLAMA"

BUILD_ROOT="$SCRATCH_DIR/build"
mkdir -p "$BUILD_ROOT" \
	|| die "Cannot create the build directory:\n       $BUILD_ROOT"

BUILD_DIR="$BUILD_ROOT/llama"
if [ x$CHECKOUT_CLEAN = x1 ]; then
	p Checking out the repository
	git clone --quiet "$CHECKOUT_SOURCE" "$BUILD_DIR" \
		|| die "Cannot clone the source repository:\n       $CHECKOUT_SOURCE"
else
	ln -s "$ROOT_LLAMA" "$BUILD_DIR" \
		|| die "Cannot create the build directory symlink:\n       $BUILD_DIR"
fi

cd "$BUILD_ROOT"
for i in $LINK_TO_BUILD_DIR; do
	ln -s "$i" . \
		|| die "Cannot symlink into the build directory:\n       $i"
done

# Get the git revision again, just to make sure we got the right hash before
cd "$BUILD_DIR"
GIT_HEAD="`git rev-parse HEAD`"
cd "$ROOT_TESTS"

p "Building LLAMA (git $GIT_HEAD)"

make -C "$BUILD_DIR" 2>&1 \
	| tee -a "$RESULTS_DIR/build.txt" \
	| grep --line-buffered -E '^(CC|LD|CXX)' \
	| stdbuf -oL sed 's/^/  /' | save

if [ ${PIPESTATUS[0]} != 0 ]; then
	p
	error "Build failed. Check for details (reproduced below):\n      " \
		"$RESULTS_DIR/build.txt"
	p
	cat "$RESULTS_DIR/build.txt" | save
	p
	die "Failed."
fi


#
# Now run each benchmark on each dataset
#

p
heading2 "Running benchmarks"

NUM_ERRORS=0
NUM_WARNINGS=0
NUM_SUCCESS=0
NUM_REGRESSIONS=0

STATUS_DETAILED_CSV="$RESULTS_DIR/status-detailed.csv"
STATUS_SUMMARY_CSV="$RESULTS_DIR/status-summary.csv"

echo "program,data,task,filename_base,success,runtime_ms,overhead_percent,comment" \
	> $STATUS_DETAILED_CSV

for BP in $BENCHMARK_PROGRAMS; do
	for DF in $DATA_FILES; do
		for BT in $BENCHMARK_TASKS; do

			RBT="`echo $BT | sed 's/__/ /g'`"
			BBT="`echo $BT | sed 's/__.*$//'`"

			p "Running $BP :: `basename $DF` :: $BBT"

			# Warm up the cache for the data file
			cat $DF > /dev/null
			
			BDF="`basename $DF`"
			OUT_BASE_FILE="`echo ${BP}__${BDF}__$BBT | tr '.' '_'`"
			OUT_BASE="$RESULTS_DIR/$OUT_BASE_FILE"
			OUT_RESULTS="${OUT_BASE}.results.txt"
			OUT_OUTPUT="${OUT_BASE}.output.txt"
			OUT_STATS="${OUT_BASE}.stats.csv"
			STATUS_BASE="${BP},`basename $DF`,$BT,$OUT_BASE_FILE"

			RUN_DIR="$SCRATCH_DIR/run"
			mkdir -p "$RUN_DIR" \
				|| die "Cannot recreate the scratch context directory:\n" \
				"      $RUN_DIR"
			
			cd "$RUN_DIR"
			(time "$BUILD_DIR/bin/$BP" $BENCHMARK_ARGS -vSL "$DF" \
				-r $RBT -o "$OUT_RESULTS") \
				2>&1 | cat > $OUT_OUTPUT

			if [ ${PIPESTATUS[0]} != 0 ]; then
				NUM_ERRORS=$[$NUM_ERRORS + 1]
				p
				error "Task failed. Check for details (reproduced below):\n" \
					"      $OUT_OUTPUT"
				p
				cat "$OUT_OUTPUT" | save
				p
				echo "$STATUS_BASE,false,0,0,\"Task failed\"" \
					>> $STATUS_DETAILED_CSV
			else

				CSV_COUNT="`ls -1 $RUN_DIR/*.csv 2> /dev/null \
					| grep -cvE '^\.'`"
				if [ x$CSV_COUNT == x0 ]; then
					die "Cannot find the statistics file"
				fi
				if [ x$CSV_COUNT != x1 ]; then
					die "Too many .csv files - which one is which?"
				fi

				ORG_STATS="`ls -1 $RUN_DIR/*.csv | head -n 1`"
				/bin/cp $ORG_STATS $OUT_STATS
				RUNTIME_MS=`head -n 5 $OUT_STATS | tail -n 1 | awk -F, '{print $2}'`

				EXPECTED_RESULTS="$EXPECTED_DIRECTORY/`basename $OUT_RESULTS`"
				if [ -f $EXPECTED_RESULTS ]; then
					DIFF="`diff $EXPECTED_RESULTS $OUT_RESULTS`"
					if [ "x$DIFF" != "x" ]; then 
						NUM_ERRORS=$[$NUM_ERRORS + 1]
						p
						error "Task failed because it outputted wrong results"
						p
						echo "$STATUS_BASE,false,0,0,\"Wrong results\"" \
							>> $STATUS_DETAILED_CSV
					else
						FASTEST_MS=`(echo $RUNTIME_MS ; \
							(fgrep $STATUS_BASE,true, $ROOT_RESULTS/*/status-detailed.csv \
							| sed 's/^.*,true,//' | awk -F, '{ print $1 }')) \
							| sort -n | head -n 1`
						OVERHEAD_P=`echo \
							"scale=2; (100.0 * ($RUNTIME_MS - $FASTEST_MS)) / $FASTEST_MS" \
							| bc`
						OVERHEAD_P_I=`echo $OVERHEAD_P | awk -F '.' '{ print $1 }'`
						if [ x$OVERHEAD_P_I == x ]; then
							OVERHEAD_P_I=0
						fi
						if [ $OVERHEAD_P_I -gt 5 ]; then
							NUM_REGRESSIONS=$[$NUM_REGRESSIONS+ 1]
							echo "$STATUS_BASE,true,$RUNTIME_MS,$OVERHEAD_P,\"Regression\"" \
								>> $STATUS_DETAILED_CSV
							p
							warning "Performance regression:\n" \
								"        Runtime : $RUNTIME_MS ms\n" \
								"        Fastest : $FASTEST_MS ms\n" \
								"        Overhead: $OVERHEAD_P %"
							p
						else
							NUM_SUCCESS=$[$NUM_SUCCESS + 1]
							echo "$STATUS_BASE,true,$RUNTIME_MS,$OVERHEAD_P,\"Success\"" \
								>> $STATUS_DETAILED_CSV
						fi
					fi
				else
					NUM_WARNINGS=$[$NUM_WARNINGS + 1]
					p
					warning "The expected results file not found:\n" \
						"        $EXPECTED_RESULTS"
					p
					echo "$STATUS_BASE,false,0,0,\"No result file\"" \
						>> $STATUS_DETAILED_CSV
				fi
			fi

			cd "$ROOT_TESTS"

			/bin/rm -rf "$RUN_DIR" \
				|| die "Cannot remove the scratch context directory"
		done
	done
done


#
# Finish by writing the status file
#

echo "errors,warnings,successes,regressions" > $STATUS_SUMMARY_CSV
echo "$NUM_ERRORS,$NUM_WARNINGS,$NUM_SUCCESS,$NUM_REGRESSIONS" >> $STATUS_SUMMARY_CSV


#
# Cleanup
#

/bin/rm -rf "$SCRATCH_DIR" || die "Cannot remove the scratch directory"

p

if [ x$NUM_ERRORS != x0 ]; then
	error "There were errors."
elif [ x$NUM_WARNINGS != x0 ]; then
	warning "There were warnings."
elif [ x$NUM_REGRESSIONS != x0 ]; then
	warning "There were performance regressions."
else
	success Success.
fi

$ROOT_TESTS/generate-report.py -d $ROOT_RESULTS \
	|| error "Cannot generate the report"
