Instructions to run Souffle benchmarks:
1. Extract each `.tar.bz2` in `data`
2. For each Souffle version (`souffle`, `souffle-google-btree`, `souffle-intel-hashset`):
    1. Build Souffle (`sh bootstrap ; ./configure ; make -j` in each Souffle directory)
3. Run `run.sh` to run all benchmarks
    1. Note that you may need to change the line with `export GOMP_CPU_AFFINITY` to match the number of cores in your machine

Note: Running the experiment for B-tree with no hints requires modification of Souffle source code (line 1144 of `src/BTree.h` should be changed from `1` to `0`)
