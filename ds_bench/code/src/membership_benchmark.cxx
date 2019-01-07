
#include <vector>

#include <set>
#include <unordered_set>

#include "tree_set_utils.h"

#include "souffle/sequential/BTree.h"
#include "souffle/fine_grained/BTree.h"
#include "souffle/read_write/BTree.h"
#include "souffle/optimistic/BTree.h"
#include "souffle/Trie.h"

#include "google/btree_set.h"

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
const std::vector<int> problem_sizes = { 1000, 2000, 5000, 10000 };

// the range of inverse densities to test
const std::vector<int> inverse_densities = { 10 }; // 1, 10, 20, 50, 100, 200, 1000 };


// the set of data structures to be evaluated
const std::vector<std::string> structures {
//	"std::set",
//	"std::unordered_set",
//	"google_btree",
//	"tbb::concurrent_unordered_set",
//	"sequential_btree",
//	"global_lock_btree",
//	"fine_grained_lock_btree",
//	"read_write_lock_btree",
	"optimistic_btree",
//	"brie"
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

template<typename Set>
struct ContainsCheck {
	template<typename Entry>
	bool operator()(const Set& s, const Entry& e) const {
		return s.contains(e);
	}
	template<typename Entry,typename Hints>
	bool operator()(const Set& s, const Entry& e, Hints& hints) const {
		return s.contains(e,hints);
	}
};

template<typename E>
struct ContainsCheck<std::set<E>> {
	bool operator()(const std::set<E>& s, const E& e) const {
		return s.find(e) != s.end();
	}
};

template<typename E>
struct ContainsCheck<std::unordered_set<E>> {
	bool operator()(const std::unordered_set<E>& s, const E& e) const {
		return s.find(e) != s.end();
	}
};

template<typename E>
struct ContainsCheck<btree::btree_set<E>> {
	bool operator()(const btree::btree_set<E>& s, const E& e) const {
		return s.find(e) != s.end();
	}
};

template<typename E>
struct ContainsCheck<tbb::concurrent_unordered_set<E>> {
	bool operator()(const tbb::concurrent_unordered_set<E>& s, const E& e) const {
		return s.find(e) != s.end();
	}
};


template<typename Set, typename List>
auto eval_query(const List& list) {

	ContainsCheck<Set> check;

	// fill the set
	Set s;
	for(auto cur : list) {
		s.insert(cur);
	}

	// check the memberships
	return run_eval([&](){
		std::size_t count = 0;
		for(auto cur : list) {
			if (check(s,cur)) count++;
		}
		if (count != list.size()) {
			std::cout << "Consistency check failed!\nExpected: " << list.size() << " == " << count << "\n";
			exit(1);
		}
	},NUM_REPETITIONS,TIME_OUT);
}

template<typename Set, typename List>
auto eval_query_with_hints(const List& list) {

	ContainsCheck<Set> check;

	// fill the set
	Set s;
	typename Set::operation_hints hints;
	for(auto cur : list) {
		s.insert(cur,hints);
	}

	// check the memberships
	return run_eval([&](){
		std::size_t count = 0;
		for(auto cur : list) {
			if (check(s,cur,hints)) count++;
		}
		if (count != list.size()) {
			std::cout << "Consistency check failed!\nExpected: " << list.size() << " == " << count << "\n";
			exit(1);
		}
	},NUM_REPETITIONS,TIME_OUT);
}

template<typename Set, typename List>
auto eval_query_brie_with_hints(const List& list) {

	ContainsCheck<Set> check;

	// fill the set
	Set s;
	typename Set::op_context hints;
	for(auto cur : list) {
		s.insert(cur,hints);
	}

	// check the memberships
	return run_eval([&](){
		std::size_t count = 0;
		for(auto cur : list) {
			if (check(s,cur,hints)) count++;
		}
		if (count != list.size()) {
			std::cout << "Consistency check failed!\nExpected: " << list.size() << " == " << count << "\n";
			exit(1);
		}
	},NUM_REPETITIONS,TIME_OUT);

}





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
			[](const auto& list) { return eval_query<StdSet>(list); }
	));

	registerDS(createDataStructure<D>(
			"std::unordered_set",
			[](const auto& list) { return eval_query<StdUnorderedSet>(list); }
	));

	registerDS(createDataStructure<D>(
			"google_btree",
			[](const auto& list) { return eval_query<GoogleBTree>(list); }
	));

	registerDS(createDataStructure<D>(
			"tbb::concurrent_unordered_set",
			[](const auto& list) { return eval_query<TBBSet>(list); }
	));

	registerDS(createDataStructure<D>(
			"sequential_btree",
			[](const auto& list) { return eval_query<SequentialBTree>(list); },
			[](const auto& list) { return eval_query_with_hints<SequentialBTree>(list); }
	));

	registerDS(createDataStructure<D>(
			"global_lock_btree",
			[](const auto& list) { return eval_query<GlobalLockBTree>(list); },
			[](const auto& list) { return eval_query_with_hints<GlobalLockBTree>(list); }
	));

	registerDS(createDataStructure<D>(
			"fine_grained_lock_btree",
			[](const auto& list) { return eval_query<FineGrainedBTree>(list); },
			[](const auto& list) { return eval_query_with_hints<FineGrainedBTree>(list); }
	));


	registerDS(createDataStructure<D>(
			"read_write_lock_btree",
			[](const auto& list) { return eval_query<ReadWriteBTree>(list); },
			[](const auto& list) { return eval_query_with_hints<ReadWriteBTree>(list); }
	));

	registerDS(createDataStructure<D>(
			"optimistic_btree",
			[](const auto& list) { return eval_query<OptimisticBTree>(list); },
			[](const auto& list) { return eval_query_with_hints<OptimisticBTree>(list); }
	));

	registerDS(createDataStructure<D>(
			"brie",
			[](const auto& list) { return eval_query<Brie>(list); },
			[](const auto& list) { return eval_query_brie_with_hints<Brie>(list); }
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
