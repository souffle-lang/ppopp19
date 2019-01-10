/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2013, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file PrecedenceGraph.cpp
 *
 * Defines the class for precedence graph to build the precedence graph,
 * compute strongly connected components of the precedence graph, and
 * build the strongly connected component graph.
 *
 ***********************************************************************/

#pragma once

#include "AstAnalysis.h"
#include "AstRelation.h"
#include "GraphUtils.h"
#include <cstddef>
#include <iostream>
#include <map>
#include <set>
#include <stack>
#include <utility>
#include <vector>

namespace souffle {

class AstClause;
class AstTranslationUnit;

/**
 * Analysis pass computing the precedence graph of the relations of the datalog progam.
 */
class PrecedenceGraph : public AstAnalysis {
private:
    /** Adjacency list of precedence graph (determined by the dependencies of the relations) */
    Graph<const AstRelation*, AstNameComparison> backingGraph;

public:
    static constexpr const char* name = "precedence-graph";

    void run(const AstTranslationUnit& translationUnit) override;

    /** Output precedence graph in graphviz format to a given stream */
    void print(std::ostream& os) const override;

    const Graph<const AstRelation*, AstNameComparison>& graph() const {
        return backingGraph;
    }
};

/**
 * Analysis pass identifying relations which do not contribute to the computation
 * of the output relations.
 */
class RedundantRelations : public AstAnalysis {
private:
    PrecedenceGraph* precedenceGraph = nullptr;

    std::set<const AstRelation*> redundantRelations;

public:
    static constexpr const char* name = "redundant-relations";

    void run(const AstTranslationUnit& translationUnit) override;

    void print(std::ostream& os) const override;

    const std::set<const AstRelation*>& getRedundantRelations() const {
        return redundantRelations;
    }
};

/**
 * Analysis pass identifying clauses which are recursive.
 */
class RecursiveClauses : public AstAnalysis {
private:
    std::set<const AstClause*> recursiveClauses;

    /** Determines whether the given clause is recursive within the given program */
    bool computeIsRecursive(const AstClause& clause, const AstTranslationUnit& translationUnit) const;

public:
    static constexpr const char* name = "recursive-clauses";

    void run(const AstTranslationUnit& translationUnit) override;

    void print(std::ostream& os) const override;

    bool recursive(const AstClause* clause) const {
        return recursiveClauses.count(clause);
    }
};

/**
 * Analysis pass computing the strongly connected component (SCC) graph for the datalog program.
 */
class SCCGraph : public AstAnalysis {
private:
    PrecedenceGraph* precedenceGraph = nullptr;

    /** Map from node number to SCC number */
    std::map<const AstRelation*, size_t> relationToScc;

    /** Adjacency lists for the SCC graph */
    std::vector<std::set<size_t>> successors;

    /** Predecessor set for the SCC graph */
    std::vector<std::set<size_t>> predecessors;

    /** Relations contained in a SCC */
    std::vector<std::set<const AstRelation*>> sccToRelation;

    /** Recursive scR method for computing SCC */
    void scR(const AstRelation* relation, std::map<const AstRelation*, size_t>& preOrder, size_t& counter,
            std::stack<const AstRelation*>& S, std::stack<const AstRelation*>& P, size_t& numSCCs);

public:
    static constexpr const char* name = "scc-graph";

    void run(const AstTranslationUnit& translationUnit) override;

    /** Get the number of SCCs in the graph. */
    size_t getNumberOfSCCs() const {
        return sccToRelation.size();
    }

    /** Get the SCC of the given relation. */
    const size_t getSCC(const AstRelation* rel) const {
        return relationToScc.at(rel);
    }

    /** Get all successor SCCs of a given SCC. */
    const std::set<size_t>& getSuccessorSCCs(const size_t scc) const {
        return successors.at(scc);
    }

    /** Get all predecessor SCCs of a given SCC. */
    const std::set<size_t>& getPredecessorSCCs(const size_t scc) const {
        return predecessors.at(scc);
    }

    /** Get all SCCs containing a successor of a given relation. */
    const std::set<size_t> getSuccessorSCCs(const AstRelation* relation) const {
        std::set<size_t> successorSccs;
        const auto scc = relationToScc.at(relation);
        for (const auto& successor : precedenceGraph->graph().successors(relation)) {
            const auto successorScc = relationToScc.at(successor);
            if (successorScc != scc) successorSccs.insert(successorScc);
        }
        return successorSccs;
    }

    /** Get all SCCs containing a predecessor of a given relation. */
    const std::set<size_t> getPredecessorSCCs(const AstRelation* relation) const {
        std::set<size_t> predecessorSccs;
        const auto scc = relationToScc.at(relation);
        for (const auto& predecessor : precedenceGraph->graph().predecessors(relation)) {
            const auto predecessorScc = relationToScc.at(predecessor);
            if (predecessorScc != scc) predecessorSccs.insert(predecessorScc);
        }
        return predecessorSccs;
    }

    /** Get all internal relations of a given SCC. */
    const std::set<const AstRelation*> getInternalRelations(const size_t scc) const {
        return sccToRelation.at(scc);
    }

    /** Get all external output predecessor relations of a given SCC. */
    const std::set<const AstRelation*> getExternalOutputPredecessorRelations(const size_t scc) const {
        std::set<const AstRelation*> externOutPreds;
        for (const auto& relation : getInternalRelations(scc)) {
            for (const auto& predecessor : precedenceGraph->graph().predecessors(relation)) {
                if (relationToScc.at(predecessor) != scc && predecessor->isOutput()) {
                    externOutPreds.insert(predecessor);
                }
            }
        }
        return externOutPreds;
    }

    /** Get all external non-output predecessor relations of a given SCC. */
    const std::set<const AstRelation*> getExternalNonOutputPredecessorRelations(const size_t scc) const {
        std::set<const AstRelation*> externNonOutPreds;
        for (const auto& relation : getInternalRelations(scc)) {
            for (const auto& predecessor : precedenceGraph->graph().predecessors(relation)) {
                if (relationToScc.at(predecessor) != scc && !predecessor->isOutput()) {
                    externNonOutPreds.insert(predecessor);
                }
            }
        }
        return externNonOutPreds;
    }

    /** Get all external predecessor relations of a given SCC. */
    const std::set<const AstRelation*> getExternalPredecessorRelations(const size_t scc) const {
        std::set<const AstRelation*> externPreds;
        for (const auto& relation : getInternalRelations(scc)) {
            for (const auto& predecessor : precedenceGraph->graph().predecessors(relation)) {
                if (relationToScc.at(predecessor) != scc) {
                    externPreds.insert(predecessor);
                }
            }
        }
        return externPreds;
    }

    /** Get all internal output relations of a given SCC. */
    const std::set<const AstRelation*> getInternalOutputRelations(const size_t scc) const {
        std::set<const AstRelation*> internOuts;
        for (const auto& relation : getInternalRelations(scc)) {
            if (relation->isOutput()) {
                internOuts.insert(relation);
            }
        }
        return internOuts;
    }

    /** Get all internal relations of a given SCC with external successors. */
    const std::set<const AstRelation*> getInternalRelationsWithExternalSuccessors(const size_t scc) const {
        std::set<const AstRelation*> internsWithExternSuccs;
        for (const auto& relation : getInternalRelations(scc)) {
            for (const auto& successor : precedenceGraph->graph().successors(relation)) {
                if (relationToScc.at(successor) != scc) {
                    internsWithExternSuccs.insert(relation);
                    break;
                }
            }
        }
        return internsWithExternSuccs;
    }

    /** Get all internal non-output relations of a given SCC with external successors. */
    const std::set<const AstRelation*> getInternalNonOutputRelationsWithExternalSuccessors(
            const size_t scc) const {
        std::set<const AstRelation*> internNonOutsWithExternSuccs;
        for (const auto& relation : getInternalRelations(scc)) {
            if (!relation->isOutput()) {
                for (const auto& successor : precedenceGraph->graph().successors(relation)) {
                    if (relationToScc.at(successor) != scc) {
                        internNonOutsWithExternSuccs.insert(relation);
                        break;
                    }
                }
            }
        }
        return internNonOutsWithExternSuccs;
    }

    /** Get all internal input relations of a given SCC. */
    const std::set<const AstRelation*> getInternalInputRelations(const size_t scc) const {
        std::set<const AstRelation*> internIns;
        for (const auto& relation : getInternalRelations(scc)) {
            if (relation->isInput()) {
                internIns.insert(relation);
            }
        }
        return internIns;
    }

    /** Return if the given SCC is recursive. */
    bool isRecursive(const size_t scc) const {
        const std::set<const AstRelation*>& sccRelations = sccToRelation.at(scc);
        if (sccRelations.size() == 1) {
            const AstRelation* singleRelation = *sccRelations.begin();
            if (!precedenceGraph->graph().predecessors(singleRelation).count(singleRelation)) {
                return false;
            }
        }
        return true;
    }

    /** Print the SCC graph. */
    void print(std::ostream& os) const override;
};

/**
 * Analysis pass computing a topologically sorted strongly connected component (SCC) graph.
 */
class TopologicallySortedSCCGraph : public AstAnalysis {
private:
    /** The strongly connected component (SCC) graph. */
    SCCGraph* sccGraph = nullptr;

    /** The final topological ordering of the SCCs. */
    std::vector<size_t> sccOrder;

    /** Calculate the topological ordering cost of a permutation of as of yet unordered SCCs
    using the ordered SCCs. Returns -1 if the given vector is not a valid topological ordering. */
    int topologicalOrderingCost(const std::vector<size_t>& permutationOfSCCs) const;

    /** Recursive component for the forwards algorithm computing the topological ordering of the SCCs. */
    void computeTopologicalOrdering(size_t scc, std::vector<bool>& visited);

public:
    static constexpr const char* name = "topological-scc-graph";

    void run(const AstTranslationUnit& translationUnit) override;

    const std::vector<size_t>& order() const {
        return sccOrder;
    }

    const size_t sccOfIndex(const size_t index) const {
        return sccOrder.at(index);
    }

    const size_t indexOfScc(const size_t scc) const {
        auto it = std::find(sccOrder.begin(), sccOrder.end(), scc);
        assert(it != sccOrder.end());
        return (size_t)std::distance(sccOrder.begin(), it);
    }

    const std::set<size_t> indexOfScc(const std::set<size_t>& sccs) const {
        std::set<size_t> indices;
        for (const auto scc : sccs) {
            indices.insert(indexOfScc(scc));
        }
        return indices;
    }

    /** Output topologically sorted strongly connected component graph in text format */
    void print(std::ostream& os) const override;
};

/**
 * A single step in a relation schedule, consisting of the relations computed in the step
 * and the relations that are no longer required at that step.
 */
class RelationScheduleStep {
private:
    std::set<const AstRelation*> computedRelations;
    std::set<const AstRelation*> expiredRelations;
    const bool isRecursive;

public:
    RelationScheduleStep(std::set<const AstRelation*> computedRelations,
            std::set<const AstRelation*> expiredRelations, const bool isRecursive)
            : computedRelations(std::move(computedRelations)), expiredRelations(std::move(expiredRelations)),
              isRecursive(isRecursive) {}

    const std::set<const AstRelation*>& computed() const {
        return computedRelations;
    }

    const std::set<const AstRelation*>& expired() const {
        return expiredRelations;
    }

    bool recursive() const {
        return isRecursive;
    }

    void print(std::ostream& os) const;

    /** Add support for printing nodes */
    friend std::ostream& operator<<(std::ostream& out, const RelationScheduleStep& other) {
        other.print(out);
        return out;
    }
};

/**
 * Analysis pass computing a schedule for computing relations.
 */
class RelationSchedule : public AstAnalysis {
private:
    TopologicallySortedSCCGraph* topsortSCCGraph = nullptr;
    PrecedenceGraph* precedenceGraph = nullptr;

    /** Relations computed and expired relations at each step */
    std::vector<RelationScheduleStep> relationSchedule;

    std::vector<std::set<const AstRelation*>> computeRelationExpirySchedule(
            const AstTranslationUnit& translationUnit);

public:
    static constexpr const char* name = "relation-schedule";

    void run(const AstTranslationUnit& translationUnit) override;

    const std::vector<RelationScheduleStep>& schedule() const {
        return relationSchedule;
    }

    /** Dump this relation schedule to standard error. */
    void print(std::ostream& os) const override;
};

}  // end of namespace souffle
