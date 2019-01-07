#include <set>
#include <unordered_set>
#include <map>
#include <vector>

#include <omp.h>

#include "tree_set_utils.h"

#include "souffle/sequential/BTree.h"
#include "souffle/fine_grained/BTree.h"
#include "souffle/read_write/BTree.h"
#include "souffle/optimistic/BTree.h"

#include "souffle/Trie.h"

#include "google/btree_set.h"

#include "tbb/concurrent_unordered_set.h"

#include "eval.h"
#include "benchmark_utils.h"

namespace google = btree;

// --- configuration ----

int NUM_REPETITIONS = 3;
//int TIME_OUT = -1;			// (no time-out)
int TIME_OUT = 300;			// (in seconds)


// data set properties

// 1D - with 100M entries
//const int N = 100000000;
//const int D = 1;
//
// 2D - with 100M entries
const int N = 10000;
const int D = 2;
//
//// 3D - with ~100M entries
//const int N = 464;
//const int D = 3;
//
//// 4D - with 100M entries
//const int N = 100;
//const int D = 4;

//const int N = 1000;
//const int D = 2;

// the density of the data points
const std::vector<int> inverse_densities = {
	1
//	10,20,50,100,200,1000
};


// the range of threads to be evaluated
const std::vector<int> num_threads = seq(1,6);
//std::vector<int> num_threads {
//	1,2,4,8,16,24,32
//	3,5,6,7,9,10,11,12,13,14,15,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31
//};


// the set of data structures to be evaluated
const std::vector<std::string> structures {
/*	"std::set",
	"std::unordered_set",
	"google_btree",
	"tbb::concurrent_unordered_set",
	"global_lock_btree",
	"fine_grained_lock_btree",
	"read_write_lock_btree",
*/
	"optimistic_btree",
	"brie"
};

// the different kind of orders to be evaluated
const std::vector<InputType> orders {
	InputType::Ordered,		// evaluate insertion of data in order
	InputType::Unordered	// evaluate insertion of data out-of-order
};

// the hint modes to be evaluated
const std::vector<bool> hints {
	false,		// run evaluation without insertion hints
	true		// run evaluation with insertion hints
};


// ----------------------



int main() {

	using Entry = Entry<D>;

	// reference data structures
	using StdSet = ProtectedSet<std::set<Entry>>;
	using StdUnorderedSet = ProtectedSet<std::unordered_set<Entry>>;
	using GoogleBTree = ProtectedSet<google::btree_set<Entry>>;
	using TBBSet = tbb::concurrent_unordered_set<Entry>;

	// investigated data structures
	using SequentialBTree  = souffle::sequential::btree_set<Entry>;
	using GlobalLockBTree  = ProtectedSetWithHints<SequentialBTree>;
	using FineGrainedBTree = souffle::fine_grained::btree_set<Entry>;
	using ReadWriteBTree   = souffle::read_write::btree_set<Entry>;
	using OptimisticBTree  = souffle::optimistic::btree_set<Entry>;

	using Brie = souffle::Trie<D>;

	// --- Setup ---

	// create list of target data structures
	std::map<std::string,DataStructure<D>> structureRegister;

	auto registerDS = [&](const DataStructure<D>& cur) {
		structureRegister[cur.name] = cur;
	};

	registerDS(createDataStructure<D>(
			"std::set",
			[](const auto& list) { return eval_insert<StdSet>(list,NUM_REPETITIONS,TIME_OUT); }
	));

	registerDS(createDataStructure<D>(
			"std::unordered_set",
			[](const auto& list) { return eval_insert<StdUnorderedSet>(list,NUM_REPETITIONS,TIME_OUT); }
	));

	registerDS(createDataStructure<D>(
			"google_btree",
			[](const auto& list) { return eval_insert<GoogleBTree>(list,NUM_REPETITIONS,TIME_OUT); }
	));

	registerDS(createDataStructure<D>(
			"tbb::concurrent_unordered_set",
			[](const auto& list) { return eval_insert<TBBSet>(list,NUM_REPETITIONS,TIME_OUT); }
	));

	registerDS(createDataStructure<D>(
			"global_lock_btree",
			[](const auto& list) { return eval_insert<GlobalLockBTree>(list,NUM_REPETITIONS,TIME_OUT); },
			[](const auto& list) { return eval_insert_with_hints<GlobalLockBTree>(list,NUM_REPETITIONS,TIME_OUT); }
	));

	registerDS(createDataStructure<D>(
			"fine_grained_lock_btree",
			[](const auto& list) { return eval_insert<FineGrainedBTree>(list,NUM_REPETITIONS,TIME_OUT); },
			[](const auto& list) { return eval_insert_with_hints<FineGrainedBTree>(list,NUM_REPETITIONS,TIME_OUT); }
	));


	registerDS(createDataStructure<D>(
			"read_write_lock_btree",
			[](const auto& list) { return eval_insert<ReadWriteBTree>(list,NUM_REPETITIONS,TIME_OUT); },
			[](const auto& list) { return eval_insert_with_hints<ReadWriteBTree>(list,NUM_REPETITIONS,TIME_OUT); }
	));

	registerDS(createDataStructure<D>(
			"optimistic_btree",
			[](const auto& list) { return eval_insert<OptimisticBTree>(list,NUM_REPETITIONS,TIME_OUT); },
			[](const auto& list) { return eval_insert_with_hints<OptimisticBTree>(list,NUM_REPETITIONS,TIME_OUT); }
	));

	registerDS(createDataStructure<D>(
			"brie",
			[](const auto& list) { return eval_insert<Brie>(list,NUM_REPETITIONS,TIME_OUT); },
			[](const auto& list) { return eval_insert_brie_with_hints<Brie>(list,NUM_REPETITIONS,TIME_OUT); }
	));


	// --- test sequential inserts of pairs ---


	// define the data container for this experiment

	// the store for all the measured data
	Measurements results;

	// add basic configuration
	Configuration config;

	// add problem size parameters
	config.dimensions = D;
	config.problemSize = N;

	for(int inverse_density : inverse_densities) {

		std::cout << "Processing density 1/" << inverse_density << " ..\n";
		config.inverse_density = inverse_density;

		// create the input data vector
		std::cout << "Creating input vector ... \n";
		std::vector<Entry> data = generateInputData<D>(N,inverse_density);

		// also create a randomly shuffled input vector
		std::cout << "Created shuffled input vector ... \n";
		std::vector<Entry> shuffled = data;
		std::random_shuffle(shuffled.begin(),shuffled.end());

		// for the selected range of threads
		for(int t : num_threads) {

			// fix the number of threads
			omp_set_num_threads(t);

			std::cout << "\nFixing number of threads to " << (t) << "\n";

			// fix parameters
			config.numThreads = t;

			// for the set of orders to be covered
			for(const auto& order : orders) {
				config.inputType = order;
				const auto& input = (order == InputType::Ordered) ? data : shuffled;

				// for the set of structures to be evaluated
				for(const auto& cur : structures) {
					// fix the data structure
					const auto& datastructure = structureRegister[cur];
					config.dataStructureName = datastructure.name;

					// for the hint options
					for(const auto& hint : hints) {
						// fix the hint state
						config.hints = hint;

						// check
						std::cout<< "Evaluating " << config << " ... \n";
						results.add({
							config,
							(hint)
								? datastructure.evalWithContext(input)
								: datastructure.evalWithoutContext(input)
						});
					}

				}
			}

			results.dump();

		} // end num-threads loop

	} // end density loop

	// done
	return 0;
}
