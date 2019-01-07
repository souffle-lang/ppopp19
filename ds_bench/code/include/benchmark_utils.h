#pragma once

#include <vector>

#include "souffle/RamTypes.h"
#include "souffle/CompiledRamTuple.h"

#include "eval.h"

template<std::size_t D>
using Entry = souffle::ram::Tuple<souffle::RamDomain,D>;

/**
 * Creates input data of the requested dimension.
 *
 * @tparam D the dimension of the input data
 * @param N the number of elements per dimension (N^D elements will be created)
 * @param inverse_density the inverse density of the resulting points
 */
template<std::size_t D>
std::vector<Entry<D>> generateInputData(int N, int inverse_density) {

	std::vector<Entry<D>> data;

	// start with the point (0,...)
	Entry<D> cur;
	for(std::size_t i=0; i<D; ++i) {
		cur[i] = 0;
	}

	// compute total size
	std::size_t totalSize = 1;
	for(std::size_t i=0; i<D; ++i) {
		totalSize *= N;
	}

	// create total size of elements
	data.reserve(totalSize);

	for(std::size_t i = 0; i<totalSize; ++i) {
		// add current
		data.push_back(cur);

		// increment element
		int j = D-1;
		while (j>=0) {
			// make a step
			cur[j]+= (j==D-1) ? inverse_density : 1;
			if (cur[j] < N) break;
			// reset and increment next
			if (j != 0) cur[j] = cur[j] % N;
			j--;
		}
	}

	// done
	return data;
}


inline std::vector<int> seq(int a,int b,int c=1) {
	std::vector<int> res;
	for(int i=a; i<=b; i+=c) {
		res.push_back(i);
	}
	return res;
}



enum class InputType {
	Ordered, Unordered
};

template<std::size_t D>
struct DataStructure {

	std::string name;

	std::function<EvalResult(const std::vector<Entry<D>>& data)> evalWithoutContext;

	std::function<EvalResult(const std::vector<Entry<D>>& data)> evalWithContext;

	DataStructure() = default;

	template<typename EvalWithout, typename EvalWith>
	DataStructure(const std::string& name, const EvalWithout& a, const EvalWith& b)
		: name(name), evalWithoutContext(a), evalWithContext(b) {}


	bool operator<(const DataStructure& other) const {
		return name < other.name;
	}
};

template<std::size_t D, typename Eval>
DataStructure<D> createDataStructure(const std::string& name, const Eval& eval) {
	return { name, eval, eval };
}

template<std::size_t D, typename EvalWithoutContext, typename EvalWithContext>
DataStructure<D> createDataStructure(const std::string& name, const EvalWithoutContext& evalWithout, const EvalWithContext& evalWith) {
	return { name, evalWithout, evalWith };
}


std::ostream& operator<<(std::ostream& out, const InputType& type);


// a configuration point to be evaluated
struct Configuration {

	// the number of threads used for the evaluation
	int numThreads;

	// the data structure evaluated
	std::string dataStructureName;

	// the number of dimensions processed
	int dimensions;

	// the number of entries inserted
	int problemSize;

	// the type of input evaluated (ordered or unordered)
	InputType inputType;

	// the density of the input data (1 .. fully dense, 5 .. 1/5 = 20%)
	int inverse_density;

	// have insertion hints be utilized?
	bool hints;

};

std::ostream& operator<<(std::ostream& out, const Configuration& config);


// a measurement taken
struct Measurement {

	// the evaluated configuration
	Configuration config;

	// the measured result
	EvalResult result;

};

struct Measurements {

	std::vector<Measurement> results;

	void add(const Measurement& cur) {
		results.push_back(cur);
	}

	void dump() const;
};


template<typename Set, std::size_t D>
auto eval_insert(const std::vector<Entry<D>>& list, int N, int time_out = -1) {
	return run_eval([&](){
		Set s;
		#pragma omp parallel for schedule(guided,1000)
		for(std::size_t i = 0; i<list.size(); ++i) {
			s.insert(list[i]);
		}
	},N,time_out);
}

template<typename Set, std::size_t D>
auto eval_insert_with_hints(const std::vector<Entry<D>>& list, int N, int time_out = -1) {
	return run_eval([&](){
		Set s;
		#pragma omp parallel
		{
			typename Set::operation_hints hints;
			#pragma omp for
			for(std::size_t i = 0; i<list.size(); ++i) {
				s.insert(list[i],hints);
			}
		}
	},N, time_out);
}

template<typename Set, std::size_t D>
auto eval_insert_brie_with_hints(const std::vector<Entry<D>>& list, int N, int time_out = -1) {
	return run_eval([&](){
		Set s;
		#pragma omp parallel
		{
			typename Set::op_context hints;
			#pragma omp for schedule(guided)
			for(std::size_t i = 0; i<list.size(); ++i) {
				s.insert(list[i],hints);
			}
		}
	},N,time_out);
}

