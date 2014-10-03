#!/usr/bin/python

#
# generate-report.py
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


import csv
import getopt
import glob
import os.path
import sys


#
# Usage
#

def usage():
    sys.stderr.write("Usage: generate-report.py [OPTIONS]\n\n")
    sys.stderr.write("Options:\n")
    sys.stderr.write("  -d, --dir DIR     Set the results directory\n")
    sys.stderr.write("  -h, --help        Print this help message and exit\n")



#
# Global variables, settings, and parsing of the command-line arguments
#

global results_dir
results_dir = "/var/www/llama-tests"

try:
    opts, args = getopt.getopt(sys.argv[1:],"d:h",["dir=","help"])
except getopt.GetoptError as e:
    sys.stderr.write("Error: " + str(e) + "\n")
    sys.exit(2)

for opt, arg in opts:
    if opt in ("-h", "--help"):
        usage()
        sys.exit()
    elif opt in ("-d", "--dir"):
        results_dir = arg



#
# Utilities
#

def ife(b, t, f):
    if b:
        return t
    else:
        return f



#
# Basic CSV functionality
#

class csv_row_array:

    def __init__(self, file_name):
        """
        Initialize the object from the given file name
        """

        self.file_name = file_name;
        self.header = []
        self.header_dict = dict()
        self.rows = []

        first = True
        with open(file_name, "rb") as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',', quotechar='\"')
            for row in csv_reader:
                if first:
                    self.header = row
                    for i in xrange(0, len(row)):
                        self.header_dict[row[i]] = i
                    first = False;
                else:
                    self.rows.append(row);



#
# Find all runs
#

report_file = results_dir + "/report.html"
out = open(report_file, "w")

out.write("<html>\n")
out.write("<head>\n")
out.write("  <title>LLAMA Test Results Summary</title>\n")
out.write("  <style>\n")
out.write("    table, th, td {\n")
out.write("      border: 1px solid black;\n")
out.write("      border-collapse: collapse;\n")
out.write("      padding: 3px;\n")
out.write("    }\n")
out.write("    td          { text-align: center; }\n")
out.write("    .run a      { font-weight: normal; text-align: left; }\n")
out.write("    .success    { color: green; font-weight: bold; }\n")
out.write("    .warning    { color: orange; font-weight: bold; }\n")
out.write("    .error      { color: red; font-weight: bold; }\n")
out.write("    .regression { color: orange; font-weight: bold; }\n")
out.write("  </style>\n")
out.write("</head>\n")
out.write("<body>\n")
out.write(" <table>\n")
out.write("   <tr>\n")
out.write("     <th>Test Run</th>\n")
out.write("     <th>Total Runs</th>\n")
out.write("     <th>Successes</th>\n")
out.write("     <th>Warnings</th>\n")
out.write("     <th>Errors</th>\n")
out.write("     <th>Regressions</th>\n")
out.write("   </tr>\n")

run_dirs = []
for r in glob.glob(results_dir + "/20*"):
    if not os.path.isdir(r): continue
    run_dirs.append(r)

run_dirs.sort()

for run_dir in run_dirs:

    out.write("   <tr>\n")
    out.write("     <th class=\"run\"><a href=\"%s\">%s</a></th>\n" %
        (os.path.basename(run_dir) + "/report.txt", os.path.basename(run_dir)))

    fn_status_details = run_dir + "/status-detailed.csv"
    if not os.path.isfile(fn_status_details):
        sys.stderr.write("Error: No details file for " + run_dir + "\n");
        out.write("     <td colspan=\"5\" class=\"error\">No details file</td>\n")
        out.write("   </tr>\n")
        continue
    status_details = csv_row_array(fn_status_details)

    fn_status_summary = run_dir + "/status-summary.csv"
    if not os.path.isfile(fn_status_summary):
        sys.stderr.write("Error: No summary file for " + run_dir + "\n");
        out.write("     <td colspan=\"5\" class=\"error\">No summary file</td>\n")
        out.write("   </tr>\n")
        continue
    status_summary = csv_row_array(fn_status_summary)

    s_sum_row       = status_summary.rows[0]
    num_errors      = int(s_sum_row[status_summary.header_dict["errors"]])
    num_warnings    = int(s_sum_row[status_summary.header_dict["warnings"]])
    num_successes   = int(s_sum_row[status_summary.header_dict["successes"]])
    num_regressions = int(s_sum_row[status_summary.header_dict["regressions"]])

    out.write("     <td>%d</td>\n" % (num_successes + num_warnings + num_errors +
        num_regressions))
    out.write("     <td%s>%d</td>\n" %
            (ife(num_successes != 0, " class=\"success\"", ""), num_successes))
    out.write("     <td%s>%d</td>\n" %
            (ife(num_warnings != 0, " class=\"warning\"", ""), num_warnings))
    out.write("     <td%s>%d</td>\n" %
            (ife(num_errors != 0, " class=\"error\"", ""), num_errors))
    out.write("     <td%s>%d</td>\n" %
            (ife(num_regressions != 0, " class=\"regression\"", ""), num_regressions))
    out.write("   </tr>\n")

out.write(" </table>\n")
out.write("</body>\n")

out.close()
print "Generated: " + report_file

