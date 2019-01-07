
#include <vector>

#include <set>
#include <unordered_set>

#include <omp.h>

#include "tree_set_utils.h"

#include "souffle/sequential/BTree.h"
#include "souffle/fine_grained/BTree.h"
#include "souffle/read_write/BTree.h"
#include "souffle/optimistic/BTree.h"

#include "souffle/Trie.h"

#include "google/btree_set.h"

#include "tbb/concurrent_unordered_set.h"

namespace google = btree;

#include "eval.h"
#include "benchmark_utils.h"

// -- Configuration --------------------------------------

int NUM_REPETITIONS = 3;
int TIME_OUT = -1;			// (no time-out)
//int TIME_OUT = 1;			// (in seconds)

// the dimension of input data to test
const int D = 2;

// the range of problem sizes to test
const std::vector<int> problem_sizes = { 1000 };

// the range of inverse densities to test
const std::vector<int> inverse_densities = { 1 };


// the set of data structures to be evaluated
const std::vector<std::string> structures {
	"std::set",
	"std::unordered_set",
	"google_btree",
	"tbb::concurrent_unordered_set",
	"sequential_btree",
	"global_lock_btree",
	"fine_grained_lock_btree",
	"read_write_lock_btree",
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



// -- Evaluation -----------------------------------------

int main() {

	using Entry = Entry<D>;

	using StdSet = std::set<Entry>;
	using StdUnorderedSet = std::unordered_set<Entry>;

	using GoogleBTree = google::btree_set<Entry>;

	using SequentialBTree  = souffle::sequential::btree_set<Entry>;
	using GlobalLockBTree  = ProtectedSetWithHints<SequentialBTree>;
	using FineGrainedBTree = souffle::fine_grained::btree_set<Entry>;
	using ReadWriteBTree   = souffle::read_write::btree_set<Entry>;
	using OptimisticBTree  = souffle::optimistic::btree_set<Entry>;

	using Brie = souffle::Trie<D>;

	using TBBSet = tbb::concurrent_unordered_set<Entry>;


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
			"sequential_btree",
			[](const auto& list) { return eval_insert<SequentialBTree>(list,NUM_REPETITIONS,TIME_OUT); },
			[](const auto& list) { return eval_insert_with_hints<SequentialBTree>(list,NUM_REPETITIONS,TIME_OUT); }
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


	Measurements results;

	Configuration config;
	config.numThreads = 1;
	config.dimensions = D;

	// fix the number of threads
	omp_set_num_threads(1);

	for(const int N : problem_sizes) {
		config.problemSize = N;

		for(const int id : inverse_densities) {
			config.inverse_density = id;

			// create the input data vector
			std::cout << "Creating input vector ... \n";
			std::vector<Entry> data = generateInputData<D>(N,id);

			// also create a randomly shuffled input vector
			std::cout << "Created shuffled input vector ... \n";
			std::vector<Entry> shuffled = data;
			std::random_shuffle(shuffled.begin(),shuffled.end());

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

		}
	}

	return 0;
}
