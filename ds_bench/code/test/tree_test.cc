#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <unordered_set>
#include <vector>

#include "souffle/sequential/BTree.h"
#include "souffle/fine_grained/BTree.h"
#include "souffle/read_write/BTree.h"
#include "souffle/optimistic/BTree.h"
#include "souffle/Trie.h"
#include "souffle/CompiledRamTuple.h"

#include "google/btree_set.h"

#include "tree_set_utils.h"

using namespace std;

namespace google = btree;

//const int N = 10000;
const int N = 1000000;

const int D = 1;
using Value = souffle::RamDomain;
using Entry = souffle::ram::Tuple<Value, D>;



template <typename Tree>
class TreeTest : public ::testing::Test {

};

template <typename Tree>
class ParallelTreeTest : public ::testing::Test {

};

TYPED_TEST_CASE_P(TreeTest);
TYPED_TEST_CASE_P(ParallelTreeTest);


TYPED_TEST_P(TreeTest, InsertTest) {

	using Tree = TypeParam;

	// create test vector
	vector<Entry> in;
	for(int i=0; i<N; ++i) {
		in.push_back(Entry{i});
	}

	// shuffle the input
	vector<Entry> input = in;
	std::random_shuffle(input.begin(),input.end());

	// insert elements into tree
	Tree tree;
	for(const auto& cur : input) {
		tree.insert(cur);
	}

	// retrieve the result
	vector<Entry> out(tree.begin(),tree.end());

	std::sort(out.begin(),out.end());

	// input and output should be identical
	EXPECT_EQ(in,out);
}

REGISTER_TYPED_TEST_CASE_P(TreeTest,
	InsertTest
);

typedef ::testing::Types<
		std::set<Entry>,
		std::unordered_set<Entry>,
		google::btree_set<Entry>,
		souffle::sequential::btree_set<Entry>,
		souffle::fine_grained::btree_set<Entry>,
		souffle::read_write::btree_set<Entry>,
		souffle::optimistic::btree_set<Entry>,
		souffle::Trie<1>
> TreeTypes;

INSTANTIATE_TYPED_TEST_CASE_P(AllSequential, TreeTest, TreeTypes);


TYPED_TEST_P(ParallelTreeTest, InsertTest) {

	using Tree = TypeParam;

	// create test vector
	vector<Entry> in;
	for(int i=0; i<N; ++i) {
		in.push_back(Entry{i});
	}

	// shuffle the input
	vector<Entry> input = in;
	std::random_shuffle(input.begin(),input.end());

	// insert elements into tree
	Tree tree;

	#pragma omp parallel for
	for(int i=0; i<N; ++i) {
		tree.insert(input[i]);
	}

	// retrieve the result
	vector<Entry> out(tree.begin(),tree.end());

	std::sort(out.begin(),out.end());

	// input and output should be identical
	EXPECT_EQ(in,out);
}



REGISTER_TYPED_TEST_CASE_P(ParallelTreeTest,
	InsertTest
);


typedef ::testing::Types<
		ProtectedSet<std::set<Entry>>,
		ProtectedSet<std::unordered_set<Entry>>,
		ProtectedSet<google::btree_set<Entry>>,
		ProtectedSet<souffle::sequential::btree_set<Entry>>,
		souffle::fine_grained::btree_set<Entry>,
		souffle::read_write::btree_set<Entry>,
		souffle::optimistic::btree_set<Entry>,
		souffle::Trie<1>
> ParallelTreeTypes;

INSTANTIATE_TYPED_TEST_CASE_P(AllParallel, ParallelTreeTest, ParallelTreeTypes);


TEST(OptimisticLockingTree,AtomicFootPrint) {

	EXPECT_EQ(sizeof(souffle::ram::Tuple<Value, 1>),sizeof(std::atomic<souffle::ram::Tuple<Value, 1>>));
	EXPECT_EQ(sizeof(souffle::ram::Tuple<Value, 2>),sizeof(std::atomic<souffle::ram::Tuple<Value, 2>>));
	EXPECT_EQ(sizeof(souffle::ram::Tuple<Value, 3>),sizeof(std::atomic<souffle::ram::Tuple<Value, 3>>));
	EXPECT_EQ(sizeof(souffle::ram::Tuple<Value, 4>),sizeof(std::atomic<souffle::ram::Tuple<Value, 4>>));
	EXPECT_EQ(sizeof(souffle::ram::Tuple<Value, 5>),sizeof(std::atomic<souffle::ram::Tuple<Value, 5>>));
	EXPECT_EQ(sizeof(souffle::ram::Tuple<Value, 6>),sizeof(std::atomic<souffle::ram::Tuple<Value, 6>>));
	EXPECT_EQ(sizeof(souffle::ram::Tuple<Value, 7>),sizeof(std::atomic<souffle::ram::Tuple<Value, 7>>));

	EXPECT_TRUE((std::atomic<souffle::ram::Tuple<Value,1>>().is_lock_free()));
	EXPECT_TRUE((std::atomic<souffle::ram::Tuple<Value,2>>().is_lock_free()));
//	EXPECT_TRUE((std::atomic<souffle::ram::Tuple<Value,3>>().is_lock_free()));
//	EXPECT_TRUE((std::atomic<souffle::ram::Tuple<Value,4>>().is_lock_free()));
//	EXPECT_TRUE((std::atomic<souffle::ram::Tuple<Value,5>>().is_lock_free()));
//	EXPECT_TRUE((std::atomic<souffle::ram::Tuple<Value,6>>().is_lock_free()));
//	EXPECT_TRUE((std::atomic<souffle::ram::Tuple<Value,7>>().is_lock_free()));
}

