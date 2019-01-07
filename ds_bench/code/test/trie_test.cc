#include <gtest/gtest.h>

#include "souffle/Trie.h"



TEST(Trie, Partition_1D) {

	using trie_type = souffle::Trie<1>;

	trie_type trie;
	for(int i=0; i<100; ++i) {
		trie.insert({i});
	}

	auto chunks = trie.partition(10);

	for(const auto& cur : chunks) {
		for(const auto& x : cur) {
			std::cout << x << ", ";
		}
		std::cout << "\n";
	}

}

TEST(Trie, Partition_2D) {

	using trie_type = souffle::Trie<2>;

	trie_type trie;
	for(int i=0; i<10; ++i) {
		for(int j=0; j<10; ++j) {
			trie.insert({i,j});
		}
	}

	auto chunks = trie.partition(10);

	for(const auto& cur : chunks) {
		for(const auto& x : cur) {
			std::cout << x << ", ";
		}
		std::cout << "\n";
	}

}
