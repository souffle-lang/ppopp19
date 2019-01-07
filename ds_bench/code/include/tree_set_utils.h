#pragma once

#include <mutex>

/**
 * A wrapper on top of other set implementations being protected
 * for mutal insertions.
 */
template<typename Set, typename Mutex = std::mutex>
class ProtectedSet {

	Mutex lock;

	Set data;

public:

	bool contains(const typename Set::value_type& value) const {
		return data.end() != data.find(value);
	}

	void insert(const typename Set::value_type& value) {
		std::lock_guard<Mutex> lease(lock);
		data.insert(value);
	}

	auto begin() const {
		return data.begin();
	}

	auto end() const {
		return data.end();
	}
};


/**
 * A wrapper on top of other set implementations being protected
 * for mutal insertions.
 */
template<typename Set, typename Mutex = std::mutex>
class ProtectedSetWithHints {

	Mutex lock;

	Set data;

public:

	using operation_hints = typename Set::operation_hints;

	bool contains(const typename Set::value_type& value) const {
		return data.end() != data.find(value);
	}

	bool contains(const typename Set::value_type& value, operation_hints& hints) const {
		return data.contains(value,hints);
	}

	void insert(const typename Set::value_type& value) {
		std::lock_guard<Mutex> lease(lock);
		data.insert(value);
	}

	void insert(const typename Set::value_type& value, operation_hints& hints) {
		std::lock_guard<Mutex> lease(lock);
		data.insert(value, hints);
	}

	auto begin() const {
		return data.begin();
	}

	auto end() const {
		return data.end();
	}
};
