#!/bin/bash

# Configuration
BENCHES=(antlr bloat chart eclipse fop hsqldb jython luindex lusearch pmd xalan)
RESULTS="results.txt"
THREADS=(1 2 4 8 16)
COMPILATION_NEEDED=true

# Souffle compilers
SOUFFLE="souffle/src/souffle"
SOUFFLE_GOOGLE_BTREE="souffle-google-btree/src/souffle"
SOUFFLE_INTEL_HASHSET="souffle-intel-hashset/src/souffle"

TIME_UTIL="/usr/bin/time"

[[ ! -x "$SOUFFLE" ]] && echo "Please build souffle" && exit 1
[[ ! -x "$SOUFFLE_GOOGLE_BTREE" ]] && echo "Please build souffle-google-btree" && exit 1
[[ ! -x "$SOUFFLE_INTEL_HASHSET" ]] && echo "Please build souffle-intel-hashset" && exit 1
[[ ! -x "$TIME_UTIL" ]] && echo "Cannot find /usr/bin/time" && exit 1
[[ ! -d "data/antlr" ]] && echo "Please extract data files" && exit 1

# ================
# Compilation of Souffle executables
# ================

if [[ "$COMPILATION_NEEDED" == true ]]; then
    "$SOUFFLE" -dbtree -F facts -o run-btree -j4 1-object-sensitive+heap.dl
    "$SOUFFLE" -drbtset -F facts -o run-rbtset -j4 1-object-sensitive+heap.dl
    "$SOUFFLE" -dhashset -F facts -o run-hashset -j4 1-object-sensitive+heap.dl
    "$SOUFFLE_GOOGLE_BTREE" -drbtset -F facts -o run-google-btree -j4 1-object-sensitive+heap.dl
    "$SOUFFLE_INTEL_HASHSET" -dhashset -F facts -o run-intel-hashset -j4 1-object-sensitive+heap.dl
fi

# ================
# Running benchmarks
# ================

# Header
echo "benchmark,datastructure,threads,time,memory" >> "$RESULTS"

# OpenMP CPU affinity
# CHANGE THIS FOR YOUR NUMBER OF CORES
export GOMP_CPU_AFFINITY="0-31"

for b in "${BENCHES[@]}"; do
    echo "Running benchmark $b"

    # symlink input since -F flag sometimes doesn't work on compiled executable
    ln -s "data/$b/facts" "facts"

    for t in "${THREADS[@]}"; do
        "$TIME_UTIL" -f "$b,btree,$t,%E,%M" -ao "$RESULTS" ./run-btree -j"$t"
        "$TIME_UTIL" -f "$b,rbtset,$t,%E,%M" -ao "$RESULTS" ./run-rbtset -j"$t"
        "$TIME_UTIL" -f "$b,hashset,$t,%E,%M" -ao "$RESULTS" ./run-hashset -j"$t"
        "$TIME_UTIL" -f "$b,google-btree,$t,%E,%M" -ao "$RESULTS" ./run-google-btree -j"$t"
        "$TIME_UTIL" -f "$b,intel-hashset,$t,%E,%M" -ao "$RESULTS" ./run-intel-hashset -j"$t"
    done

    # remove facts symlink
    rm facts
done
