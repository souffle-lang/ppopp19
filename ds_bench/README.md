To build them, 
- create a build directory (e.g. build_release)
- change into the build directory
- run the cmake-invocation script using `../createRelease.sh`
- build the code (`make -j`)
- run the tests (`make test`)

Now you have the benchmarks for the various test cases. Those benchmarks include:
- insert_benchmark ... benchmark for sequential insertion performance, no data structure locked
- parallel_insert_benchmark ... for parallel insertion, all properly locked
- parallel_insert_thread_private_benchmark ... a benchmark creating thread-local sets, inserting elements in those, and merging them in a reduction step; no locks, but extra work;
- scan_benchmark ... just to time the performance of iterating through a data structure
- membership_benchmark ... for benchmarking search performance
- memory_usage_benchmark ... to see how much memory is used by various data structures
