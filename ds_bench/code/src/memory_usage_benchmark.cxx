
#include <vector>

#include <set>

#include "souffle/optimistic/BTree.h"
#include "souffle/Trie.h"

#include "google/btree_set.h"
#include "tbb/concurrent_unordered_set.h"

namespace google = btree;

#include <memory>
#include <set>
#include <unordered_set>

#include "benchmark_utils.h"

// ---------------------------- Configuration ----------------------------

// max memory to be consumed (cut-off)
const std::size_t MEMORY_LIMIT = 32*1024*1024*1024ull;   // 32 GB

// number of dimensions
const int D = 2;

// the number of elements to be inserted (at most)
//const std::size_t MAX_ELEMENTS = 100000000;	// MAX_ELEMENTS ^ D elements inserted		-- for 1D
const std::size_t MAX_ELEMENTS = 10000;			// MAX_ELEMENTS ^ D elements inserted		-- for 2D
//const std::size_t MAX_ELEMENTS = 464;			// MAX_ELEMENTS ^ D elements inserted		-- for 3D
//const std::size_t MAX_ELEMENTS = 100;			// MAX_ELEMENTS ^ D elements inserted		-- for 4D

// the densities to be evaluated
const std::vector<int> INVERSE_DENSITIES = { 1, 10, 20, 50, 64, 100, 200, 500, 1000 };

// the kind of input types to be tested
const std::vector<InputType> ORDERS = {
	InputType::Ordered,
	InputType::Unordered		// = random input
};

const std::vector<std::string> DATA_STRUCTURES = {
	"std::set",
	"std::unordered_set",
	"tbb::unordered_set",
	"google btree",
	"btree",
	"brie"
};

// -----------------------------------------------------------------------

// -- utilities for measuring the memory usage of standard containers --

template<typename Key, template<typename T> class allocator = std::allocator>
struct counting_allocator : public allocator<Key> {

	using super = allocator<Key>;

	using size_type = typename super::size_type;
	using pointer = typename super::pointer;
	using const_pointer = typename super::const_pointer;


	template <class U> struct rebind { typedef counting_allocator<U> other; };

	std::shared_ptr<std::size_t> usage;

	template<typename T, template<typename S> class A>
	friend class counting_allocator;

	counting_allocator() : usage(std::make_shared<std::size_t>(0)) {}

	template<typename T>
	counting_allocator(const counting_allocator<T>& other)
		: super(other), usage(other.usage) {}

	counting_allocator(const counting_allocator&) = default;
	counting_allocator(counting_allocator&&) = default;

	auto allocate(size_type s, const_pointer hint = 0) {
		*usage += s * sizeof(Key);
		return super::allocate(s,hint);
	}

	void deallocate(pointer p, size_type n) {
		*usage -= n * sizeof(Key);
		super::deallocate(p,n);
	}

	std::size_t getMemoryUsage() const {
		return *usage;
	}
};


template<typename Set>
class MemoryAwareContainer : public Set {

public:

	std::size_t getMemoryUsage() const {
		return Set::get_allocator().getMemoryUsage();
	}

};


struct Config {
	std::string data_structure;
	int dimensions;
	InputType order;
	int inverse_density;

	friend std::ostream& operator<<(std::ostream& out, const Config& config) {
		out << config.data_structure << ",";
		out << config.dimensions << ",";
		out << config.order << ",";
		out << config.inverse_density;
		return out;
	}
};


struct Sample {
	Config config;
	std::size_t size;
	std::size_t memory_usage;

	friend std::ostream& operator<<(std::ostream& out, const Sample& sample) {
		out << sample.config << ",";
		out << sample.size << ",";
		out << sample.memory_usage;
		return out;
	}
};

struct Samples : public std::vector<Sample> {

	void add(const Config& config, std::size_t elements, std::size_t memory) {
		push_back({config,elements,memory});
	}

	friend std::ostream& operator<<(std::ostream& out, const Samples& samples) {
		out << "--------- All Results ---------\n";
		out << "DS,Dimensions,Order,inverse_density,N,memory_usage\n";
		for(const auto& cur : samples) {
			out << cur << "\n";
		}
		out << "-------------------------------\n";
		return out;
	}
};



template<typename Set, typename List>
void eval(Samples& results, const Config& config, const List& input, std::size_t mlimit = MEMORY_LIMIT) {

	std::cout << "Processing configuration " << config << "\n";

	int step = input.size() / 100;

	Set s;
	std::size_t counter = 0;
	std::cout << counter << "," << s.getMemoryUsage() << "\n";
	results.add(config,0,s.getMemoryUsage());
	for(const auto& cur : input) {
		s.insert(cur);
		counter++;
		if (counter % step == 0 || counter == input.size()) {
			auto msize = s.getMemoryUsage();
			std::cout << counter << "," << msize << "\n";
			results.add(config,counter,msize);

			// stop if limit has been crossed
			if (msize > mlimit) return;
		}
	}

}




using EvalFun = std::function<void(const Config& config, const std::vector<Entry<D>>&)>;


int main() {

	using Entry = Entry<D>;

	std::vector<std::vector<std::size_t>> data;

	using BTree = souffle::optimistic::btree_set<Entry>;
	using Brie = souffle::Trie<D>;

	using StdSet = MemoryAwareContainer<std::set<Entry, std::less<Entry>, counting_allocator<Entry>>>;
	using StdUnorderedSet = MemoryAwareContainer<std::unordered_set<Entry, std::hash<Entry>, std::equal_to<Entry>, counting_allocator<Entry>>>;

	using GoogleBtree = MemoryAwareContainer<google::btree_set<Entry,std::less<Entry>,counting_allocator<Entry>>>;
	using TBBUnorderedSet = MemoryAwareContainer<tbb::concurrent_unordered_set<Entry, tbb::tbb_hash<Entry>, std::equal_to<Entry>, counting_allocator<Entry,tbb::tbb_allocator>>>;

	// create result struct
	Samples results;

	// configure data structures
	std::map<std::string,EvalFun> structures;

	structures["std::set"]           = [&](const Config& config, const std::vector<Entry>& inputs) { eval<StdSet>(results,config,inputs); };
	structures["std::unordered_set"] = [&](const Config& config, const std::vector<Entry>& inputs) { eval<StdUnorderedSet>(results,config,inputs); };
	structures["tbb::unordered_set"] = [&](const Config& config, const std::vector<Entry>& inputs) { eval<TBBUnorderedSet>(results,config,inputs); };
	structures["google btree"]       = [&](const Config& config, const std::vector<Entry>& inputs) { eval<GoogleBtree>(results,config,inputs); };
	structures["btree"]              = [&](const Config& config, const std::vector<Entry>& inputs) { eval<BTree>(results,config,inputs); };
	structures["brie"]               = [&](const Config& config, const std::vector<Entry>& inputs) { eval<Brie>(results,config,inputs); };


	Config config;
	config.dimensions = D;

	// for each density
	for(const int& curDensity : INVERSE_DENSITIES) {
		config.inverse_density = curDensity;

		// create input data
		auto ordered = generateInputData<D>(MAX_ELEMENTS,curDensity);
		auto shuffled = ordered;
		std::random_shuffle(shuffled.begin(),shuffled.end());

		// test data structures
		for(const auto& cur : DATA_STRUCTURES) {
			config.data_structure = cur;
			const auto& eval = structures[cur];

			// evaluate orders
			for(const auto& order : ORDERS) {
				config.order = order;
				eval(config,(order == InputType::Ordered ? ordered : shuffled));
			}
		}

		std::cout << results;
	}

//	for(int inverse_density : INVERSE_DENSITIES) {
//
//		for(const auto& cur : structures) {
//			std::cout << "\nFilling " << cur.name << " with density 1/" << inverse_density << " ...\n";
//			cur.eval(inverse_density);
//		}
//
//	}

	std::cout << results << "\n";

	return 0;
}
