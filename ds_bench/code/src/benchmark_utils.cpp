#include "benchmark_utils.h"


std::ostream& operator<<(std::ostream& out, const InputType& type) {
	switch(type) {
	case InputType::Ordered: return out << "Ordered";
	case InputType::Unordered: return out << "Unordered";
	}
	return out << "?";
}

std::ostream& operator<<(std::ostream& out, const Configuration& config) {
	return out
		<< config.numThreads << ","
		<< config.dataStructureName << ","
		<< config.dimensions << ","
		<< config.problemSize << ","
		<< config.inputType << ","
		<< config.inverse_density << ","
		<< ((config.hints) ? "Hints" : "NoHints");
}


void Measurements::dump() const {
	std::cout << "\n";
	std::cout << "------ All Data --------\n";

	std::cout << "NumThreads,";
	std::cout << "DataStructure,";
	std::cout << "Dimensions,";
	std::cout << "ProblemSize,";
	std::cout << "Order,";
	std::cout << "Density,";
	std::cout << "Hints,";
	std::cout << "Time[ms]";
	std::cout << "\n";

	// print all the recorded results
	for(const auto& cur : results) {
		for(const auto& m : cur.result.measurements) {
			std::cout << cur.config << ",";
			std::cout << m;
			std::cout << "\n";
		}
	}

	std::cout << "------------------------\n";
}
