#pragma once

#include <chrono>
#include <iostream>
#include <unistd.h>

#include <sys/types.h>
#include <sys/wait.h>

inline long now() {
	return std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::high_resolution_clock::now().time_since_epoch()
	).count();
}



struct EvalResult {

	// the measured value, -1 if time-out
	std::vector<long> measurements;

	static long getTimeout() {
		return -1;
	}

	static bool isTimeout(long time) {
		return time == getTimeout();
	}

	bool hasSuccessfullRuns() const {
		for(long cur : measurements) {
			if (!isTimeout(cur)) return true;
		}
		return false;
	}

	bool hasTimeouts() const {
		for(long cur : measurements) {
			if (isTimeout(cur)) return true;
		}
		return false;
	}

	long min() const {
		long min = std::numeric_limits<long>::max();
		for(long cur : measurements) {
			if (isTimeout(cur)) return -1;
			min = std::min(min,cur);
		}
		return min;
	}

	long max() const {
		long max = 0;
		for(long cur : measurements) {
			if (isTimeout(cur)) return -1;
			max = std::max(max,cur);
		}
		return max;
	}

	long avg() const {
		long sum = 0;
		for(long cur : measurements) {
			if (isTimeout(cur)) return -1;
			sum += cur;
		}
		return sum / measurements.size();
	}
};

template<typename Case>
EvalResult run_eval(const Case& run, int N = 10, int time_out = -1) {
	EvalResult res;
	for(int i=0; i<N; i++) {

		bool success = true;
		auto start = now();

		if (time_out <= 0) {
			// just run it, no time-out
			run();
		} else {

			// run computation in different process
			auto child_id = fork();

			if (child_id == 0) {

				// this is the child, set the timeout
				alarm(time_out);
				// run task
				run();
				// and terminate this process
				exit(0);

			} else {
				// this is the parent, wait for child
				int status;
				waitpid(child_id,&status,0);
				success = WIFEXITED(status);
				if (!success) std::cout << " -- Timeout --\n";
			}

		}

		auto time = now() - start;

		if (success) {
			std::cout << "Time: " << time << "ms\n";
			res.measurements.push_back(time);
		} else {
			res.measurements.push_back(EvalResult::getTimeout());
		}

	}

	// print a summary
	if (res.hasTimeouts()) {
		std::cout << "Minimal Time: NA\n";
		std::cout << "Average Time: NA\n";
		std::cout << "Maximal Time: NA\n";
	} else {
		std::cout << "Minimal Time: " << res.min() << "ms\n";
		std::cout << "Average Time: " << res.avg() << "ms\n";
		std::cout << "Maximal Time: " << res.max() << "ms\n";
	}

	return res;
}

