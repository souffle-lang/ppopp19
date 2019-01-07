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
int TIME_OUT = -1;			// (no time-out)
//int TIME_OUT = 1;			// (in seconds)


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
const std::vector<int> num_threads = { 1, 2, 6 }; // seq(1,6);
//std::vector<int> num_threads {
//	1,2,4,8,16,24,32
//	3,5,6,7,9,10,11,12,13,14,15,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31
//};


// the set of data structures to be evaluated
const std::vector<std::string> structures {
//	"std::set (redux)",
//	"std::unordered_set (redux)",
	"google_btree (redux)",
};

// the different kind of orders to be evaluated
const std::vector<InputType> orders {
	InputType::Ordered,		// evaluate insertion of data in order
	InputType::Unordered	// evaluate insertion of data out-of-order
};

// the hint modes to be evaluated
const std::vector<bool> hints {
	false,		// run evaluation without insertion hints
//	true		// run evaluation with insertion hints
};


// ----------------------

template<typename Set>
Set& merge(Set& a, const Set& b) {
	a.insert(b.begin(),b.end());
	return a;
}

template<typename Set, std::size_t D>
auto eval_insert_reduce(const std::vector<Entry<D>>& list, int N, int time_out = -1) {

	#pragma omp declare reduction (merge:Set:merge(omp_out,omp_in))

	return run_eval([&](){
		Set s;
		#pragma omp parallel for reduction(merge:s) schedule(guided,1000)
		for(std::size_t i = 0; i<list.size(); ++i) {
			s.insert(list[i]);
		}
	},N,time_out);
}


int main() {

	using Entry = Entry<D>;

	// reference data structures
	using StdSet = std::set<Entry>;
	using StdUnorderedSet = std::unordered_set<Entry>;
	using GoogleBTree = google::btree_set<Entry>;


	// --- Setup ---

	// create list of target data structures
	std::map<std::string,DataStructure<D>> structureRegister;

	auto registerDS = [&](const DataStructure<D>& cur) {
		structureRegister[cur.name] = cur;
	};

	registerDS(createDataStructure<D>(
			"std::set (redux)",
			[](const auto& list) { return eval_insert_reduce<StdSet>(list,NUM_REPETITIONS,TIME_OUT); }
	));

	registerDS(createDataStructure<D>(
			"std::unordered_set (redux)",
			[](const auto& list) { return eval_insert_reduce<StdUnorderedSet>(list,NUM_REPETITIONS,TIME_OUT); }
	));

	registerDS(createDataStructure<D>(
			"google_btree (redux)",
			[](const auto& list) { return eval_insert_reduce<GoogleBTree>(list,NUM_REPETITIONS,TIME_OUT); }
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
