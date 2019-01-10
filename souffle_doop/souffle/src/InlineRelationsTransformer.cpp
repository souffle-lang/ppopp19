/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2018, The Souffle Developers. All rights reserved.
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file InlineRelationsTransformer.cpp
 *
 * Define classes and functionality related to inlining.
 *
 ***********************************************************************/

#include "AstArgument.h"
#include "AstClause.h"
#include "AstLiteral.h"
#include "AstNode.h"
#include "AstProgram.h"
#include "AstRelation.h"
#include "AstRelationIdentifier.h"
#include "AstTransforms.h"
#include "AstTranslationUnit.h"
#include "AstVisitor.h"
#include "BinaryConstraintOps.h"
#include "BinaryFunctorOps.h"
#include "Util.h"
#include <cassert>
#include <cstddef>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace souffle {

template <class T>
class NullableVector {
private:
    std::vector<T> vector;
    bool valid = false;

public:
    NullableVector() = default;
    NullableVector(std::vector<T> vector) : vector(std::move(vector)), valid(true) {}

    bool isValid() const {
        return valid;
    }

    const std::vector<T>& getVector() const {
        assert(valid && "Accessing invalid vector!");
        return vector;
    }
};

NullableVector<std::vector<AstLiteral*>> getInlinedLiteral(AstProgram&, AstLiteral*);

/**
 * Replace constants in the head of inlined clauses with (constrained) variables.
 */
void normaliseInlinedHeads(AstProgram& program) {
    static int newVarCount = 0;

    // Go through the clauses of all inlined relations
    for (AstRelation* rel : program.getRelations()) {
        if (!rel->isInline()) {
            break;
        }

        for (AstClause* clause : rel->getClauses()) {
            // Set up the new clause with an empty body and no arguments in the head
            auto newClause = std::make_unique<AstClause>();
            newClause->setSrcLoc(clause->getSrcLoc());
            auto clauseHead = std::make_unique<AstAtom>(clause->getHead()->getName());

            // Add in everything in the original body
            for (AstLiteral* lit : clause->getBodyLiterals()) {
                newClause->addToBody(std::unique_ptr<AstLiteral>(lit->clone()));
            }

            // Set up the head arguments in the new clause
            for (AstArgument* arg : clause->getHead()->getArguments()) {
                if (auto* constant = dynamic_cast<AstConstant*>(arg)) {
                    // Found a constant in the head, so replace it with a variable
                    std::stringstream newVar;
                    newVar << "<new_var_" << newVarCount++ << ">";
                    clauseHead->addArgument(std::make_unique<AstVariable>(newVar.str()));

                    // Add a body constraint to set the variable's value to be the original constant
                    newClause->addToBody(std::make_unique<AstBinaryConstraint>(BinaryConstraintOp::EQ,
                            std::make_unique<AstVariable>(newVar.str()),
                            std::unique_ptr<AstArgument>(constant->clone())));
                } else {
                    // Already a variable
                    clauseHead->addArgument(std::unique_ptr<AstArgument>(arg->clone()));
                }
            }

            newClause->setHead(std::move(clauseHead));

            // Replace the old clause with this one
            rel->addClause(std::move(newClause));
            program.removeClause(clause);
        }
    }
}

/**
 * Removes all underscores in all atoms of inlined relations
 */
void nameInlinedUnderscores(AstProgram& program) {
    struct M : public AstNodeMapper {
        const std::set<AstRelationIdentifier> inlinedRelations;
        bool replaceUnderscores;

        M(std::set<AstRelationIdentifier> inlinedRelations, bool replaceUnderscores)
                : inlinedRelations(std::move(inlinedRelations)), replaceUnderscores(replaceUnderscores) {}

        std::unique_ptr<AstNode> operator()(std::unique_ptr<AstNode> node) const override {
            static int underscoreCount = 0;

            if (!replaceUnderscores) {
                // Check if we should start replacing underscores for this node's subnodes
                if (auto* atom = dynamic_cast<AstAtom*>(node.get())) {
                    if (inlinedRelations.find(atom->getName()) != inlinedRelations.end()) {
                        // Atom associated with an inlined relation, so replace the underscores
                        // in all of its subnodes with named variables.
                        M replace(inlinedRelations, true);
                        node->apply(replace);
                        return node;
                    }
                }
            } else if (dynamic_cast<AstUnnamedVariable*>(node.get())) {
                // Give a unique name to the underscored variable
                // TODO (azreika): need a more consistent way of handling internally generated variables in
                // general
                std::stringstream newVarName;
                newVarName << "<underscore_" << underscoreCount++ << ">";
                return std::make_unique<AstVariable>(newVarName.str());
            }

            node->apply(*this);
            return node;
        }
    };

    // Store the names of all relations to be inlined
    std::set<AstRelationIdentifier> inlinedRelations;
    for (AstRelation* rel : program.getRelations()) {
        if (rel->isInline()) {
            inlinedRelations.insert(rel->getName());
        }
    }

    // Apply the renaming procedure to the entire program
    M update(inlinedRelations, false);
    program.apply(update);
}

/**
 * Checks if a given clause contains an atom that should be inlined.
 */
bool containsInlinedAtom(const AstProgram& program, const AstClause& clause) {
    bool foundInlinedAtom = false;

    visitDepthFirst(clause, [&](const AstAtom& atom) {
        AstRelation* rel = program.getRelation(atom.getName());
        if (rel->isInline()) {
            foundInlinedAtom = true;
        }
    });

    return foundInlinedAtom;
}

/**
 * Reduces a vector of substitutions.
 * Returns false only if matched argument pairs are found to be incompatible.
 */
bool reduceSubstitution(std::vector<std::pair<AstArgument*, AstArgument*>>& sub) {
    // Type-Checking functions
    auto isConstant = [&](AstArgument* arg) { return (dynamic_cast<AstConstant*>(arg)); };
    auto isRecord = [&](AstArgument* arg) { return (dynamic_cast<AstRecordInit*>(arg)); };

    // Keep trying to reduce the substitutions until we reach a fixed point.
    // Note that at this point no underscores ('_') or counters ('$') should appear.
    bool done = false;
    while (!done) {
        done = true;

        // Try reducing each pair by one step
        for (size_t i = 0; i < sub.size(); i++) {
            auto currPair = sub[i];
            AstArgument* lhs = currPair.first;
            AstArgument* rhs = currPair.second;

            // Start trying to reduce the substitution
            // Note: Can probably go further with this substitution reduction
            if (*lhs == *rhs) {
                // Get rid of redundant `x = x`
                sub.erase(sub.begin() + i);
                done = false;
            } else if (isConstant(lhs) && isConstant(rhs)) {
                // Both are constants but not equal (prev case => !=)
                // Failed to unify!
                return false;
            } else if (isRecord(lhs) && isRecord(rhs)) {
                // Note: we will not deal with the case where only one side is
                // a record and the other is a variable, as variables can be records
                // on a deeper level.
                std::vector<AstArgument*> lhsArgs = static_cast<AstRecordInit*>(lhs)->getArguments();
                std::vector<AstArgument*> rhsArgs = static_cast<AstRecordInit*>(rhs)->getArguments();

                if (lhsArgs.size() != rhsArgs.size()) {
                    // Records of unequal size can't be equated
                    return false;
                }

                // Equate all corresponding arguments
                for (size_t i = 0; i < lhsArgs.size(); i++) {
                    sub.push_back(std::make_pair(lhsArgs[i], rhsArgs[i]));
                }

                // Get rid of the record equality
                sub.erase(sub.begin() + i);
                done = false;
            } else if ((isRecord(lhs) && isConstant(rhs)) || (isConstant(lhs) && isRecord(rhs))) {
                // A record =/= a constant
                return false;
            }
        }
    }

    return true;
}

/**
 * Returns the nullable vector of substitutions needed to unify the two given atoms.
 * If unification is not successful, the returned vector is marked as invalid.
 * Assumes that the atoms are both of the same relation.
 */
NullableVector<std::pair<AstArgument*, AstArgument*>> unifyAtoms(AstAtom* first, AstAtom* second) {
    std::vector<std::pair<AstArgument*, AstArgument*>> substitution;

    std::vector<AstArgument*> firstArgs = first->getArguments();
    std::vector<AstArgument*> secondArgs = second->getArguments();

    // Create the initial unification equalities
    for (size_t i = 0; i < firstArgs.size(); i++) {
        substitution.push_back(std::make_pair(firstArgs[i], secondArgs[i]));
    }

    // Reduce the substitutions
    bool success = reduceSubstitution(substitution);
    if (success) {
        return NullableVector<std::pair<AstArgument*, AstArgument*>>(substitution);
    } else {
        // Failed to unify the two atoms
        return NullableVector<std::pair<AstArgument*, AstArgument*>>();
    }
}

/**
 * Inlines the given atom based on a given clause.
 * Returns the vector of replacement literals and the necessary constraints.
 * If unification is unsuccessful, the vector of literals is marked as invalid.
 */
std::pair<NullableVector<AstLiteral*>, std::vector<AstBinaryConstraint*>> inlineBodyLiterals(
        AstAtom* atom, AstClause* atomInlineClause) {
    bool changed = false;
    std::vector<AstLiteral*> addedLits;
    std::vector<AstBinaryConstraint*> constraints;

    // Rename the variables in the inlined clause to avoid conflicts when unifying multiple atoms
    // - particularly when an inlined relation appears twice in a clause.
    static int inlineCount = 0;

    // Make a temporary clone so we can rename variables without fear
    AstClause* atomClause = atomInlineClause->clone();

    struct VariableRenamer : public AstNodeMapper {
        int varnum;
        VariableRenamer(int varnum) : varnum(varnum) {}
        std::unique_ptr<AstNode> operator()(std::unique_ptr<AstNode> node) const override {
            if (auto* var = dynamic_cast<AstVariable*>(node.get())) {
                // Rename the variable
                auto newVar = std::unique_ptr<AstVariable>(var->clone());
                std::stringstream newName;
                newName << "<inlined_" << var->getName() << "_" << varnum << ">";
                newVar->setName(newName.str());
                return std::move(newVar);
            }
            node->apply(*this);
            return node;
        }
    };

    VariableRenamer update(inlineCount);
    atomClause->apply(update);

    inlineCount++;

    // Get the constraints needed to unify the two atoms
    NullableVector<std::pair<AstArgument*, AstArgument*>> res = unifyAtoms(atomClause->getHead(), atom);
    if (res.isValid()) {
        changed = true;
        for (std::pair<AstArgument*, AstArgument*> pair : res.getVector()) {
            constraints.push_back(new AstBinaryConstraint(BinaryConstraintOp::EQ,
                    std::unique_ptr<AstArgument>(pair.first->clone()),
                    std::unique_ptr<AstArgument>(pair.second->clone())));
        }

        // Add in the body of the current clause of the inlined atom
        for (AstLiteral* lit : atomClause->getBodyLiterals()) {
            addedLits.push_back(lit->clone());
        }
    }

    delete atomClause;

    if (changed) {
        return std::make_pair(NullableVector<AstLiteral*>(addedLits), constraints);
    } else {
        return std::make_pair(NullableVector<AstLiteral*>(), constraints);
    }
}

/**
 * Returns the negated version of a given literal
 */
AstLiteral* negateLiteral(AstLiteral* lit) {
    if (auto* atom = dynamic_cast<AstAtom*>(lit)) {
        AstNegation* neg = new AstNegation(std::unique_ptr<AstAtom>(atom->clone()));
        return neg;
    } else if (auto* neg = dynamic_cast<AstNegation*>(lit)) {
        AstAtom* atom = neg->getAtom()->clone();
        return atom;
    } else if (auto* cons = dynamic_cast<AstConstraint*>(lit)) {
        AstConstraint* newCons = cons->clone();
        newCons->negate();
        return newCons;
    } else {
        assert(false && "Unsupported literal type!");
        return nullptr;
    }
}

/**
 * Return the negated version of a disjunction of conjunctions.
 * E.g. (a1(x) ^ a2(x) ^ ...) v (b1(x) ^ b2(x) ^ ...) --into-> (!a1(x) ^ !b1(x)) v (!a2(x) ^ !b2(x)) v ...
 */
std::vector<std::vector<AstLiteral*>> combineNegatedLiterals(
        std::vector<std::vector<AstLiteral*>> litGroups) {
    std::vector<std::vector<AstLiteral*>> negation;

    // Corner case: !() = ()
    if (litGroups.empty()) {
        return negation;
    }

    std::vector<AstLiteral*> litGroup = litGroups[0];
    if (litGroups.size() == 1) {
        // !(a1 ^ a2 ^ a3 ^ ...) --into-> !a1 v !a2 v !a3 v ...
        for (auto& i : litGroup) {
            std::vector<AstLiteral*> newVec;
            newVec.push_back(negateLiteral(i));
            negation.push_back(newVec);
        }

        // Done!
        return negation;
    }

    // Produce the negated versions of all disjunctions ignoring the first recursively
    std::vector<std::vector<AstLiteral*>> combinedRHS = combineNegatedLiterals(
            std::vector<std::vector<AstLiteral*>>(litGroups.begin() + 1, litGroups.end()));

    // We now just need to add the negation of a single literal from the untouched LHS
    // to every single conjunction on the RHS to finalise creating every possible combination
    for (AstLiteral* lhsLit : litGroup) {
        for (std::vector<AstLiteral*> rhsVec : combinedRHS) {
            std::vector<AstLiteral*> newVec;
            newVec.push_back(negateLiteral(lhsLit));

            for (AstLiteral* lit : rhsVec) {
                newVec.push_back(lit);
            }

            negation.push_back(newVec);
        }
    }

    return negation;
}

/**
 * Forms the bodies that will replace the negation of a given inlined atom.
 * E.g. a(x) <- (a11(x), a12(x)) ; (a21(x), a22(x)) => !a(x) <- (!a11(x), !a21(x)) ; (!a11(x), !a22(x)) ; ...
 * Essentially, produce every combination (m_1 ^ m_2 ^ ...) where m_i is the negation of a literal in the
 * ith rule of a.
 */
std::vector<std::vector<AstLiteral*>> formNegatedLiterals(AstProgram& program, AstAtom* atom) {
    // Constraints added to unify atoms should not be negated and should be added to
    // all the final rule combinations produced, and so should be stored separately.
    std::vector<std::vector<AstLiteral*>> addedBodyLiterals;
    std::vector<std::vector<AstBinaryConstraint*>> addedConstraints;

    // Go through every possible clause associated with the given atom
    for (AstClause* inClause : program.getRelation(atom->getName())->getClauses()) {
        // Form the replacement clause by inlining based on the current clause
        std::pair<NullableVector<AstLiteral*>, std::vector<AstBinaryConstraint*>> inlineResult =
                inlineBodyLiterals(atom, inClause);
        NullableVector<AstLiteral*> replacementBodyLiterals = inlineResult.first;
        std::vector<AstBinaryConstraint*> currConstraints = inlineResult.second;

        if (!replacementBodyLiterals.isValid()) {
            // Failed to unify, so just move on
            continue;
        }

        addedBodyLiterals.push_back(replacementBodyLiterals.getVector());
        addedConstraints.push_back(currConstraints);
    }

    // We now have a list of bodies needed to inline the given atom.
    // We want to inline the negated version, however, which is done using De Morgan's Law.
    std::vector<std::vector<AstLiteral*>> negatedAddedBodyLiterals =
            combineNegatedLiterals(addedBodyLiterals);

    // Add in the necessary constraints to all the body literals
    for (auto& negatedAddedBodyLiteral : negatedAddedBodyLiterals) {
        for (std::vector<AstBinaryConstraint*> constraintGroup : addedConstraints) {
            for (AstBinaryConstraint* constraint : constraintGroup) {
                negatedAddedBodyLiteral.push_back(constraint->clone());
            }
        }
    }

    // Free up the old body literals and constraints
    for (std::vector<AstLiteral*> litGroup : addedBodyLiterals) {
        for (AstLiteral* lit : litGroup) {
            delete lit;
        }
    }
    for (std::vector<AstBinaryConstraint*> consGroup : addedConstraints) {
        for (AstConstraint* cons : consGroup) {
            delete cons;
        }
    }

    return negatedAddedBodyLiterals;
}

/**
 * Renames all variables in a given argument uniquely.
 */
void renameVariables(AstArgument* arg) {
    static int varCount = 0;
    varCount++;

    struct M : public AstNodeMapper {
        int varnum;
        M(int varnum) : varnum(varnum) {}
        std::unique_ptr<AstNode> operator()(std::unique_ptr<AstNode> node) const override {
            if (auto* var = dynamic_cast<AstVariable*>(node.get())) {
                auto newVar = std::unique_ptr<AstVariable>(var->clone());
                std::stringstream newName;
                newName << var->getName() << "-v" << varnum;
                newVar->setName(newName.str());
                return std::move(newVar);
            }
            node->apply(*this);
            return node;
        }
    };

    M update(varCount);
    arg->apply(update);
}

// Performs a given binary op on a list of aggregators recursively.
// E.g. ( <aggr1, aggr2, aggr3, ...>, o > = (aggr1 o (aggr2 o (agg3 o (...))))
AstArgument* combineAggregators(std::vector<AstAggregator*> aggrs, BinaryOp fun) {
    // Due to variable scoping issues with aggregators, we rename all variables uniquely in the
    // added aggregator
    renameVariables(aggrs[0]);

    if (aggrs.size() == 1) {
        return aggrs[0];
    }

    AstArgument* rhs = combineAggregators(std::vector<AstAggregator*>(aggrs.begin() + 1, aggrs.end()), fun);

    AstArgument* result = new AstBinaryFunctor(
            fun, std::unique_ptr<AstArgument>(aggrs[0]), std::unique_ptr<AstArgument>(rhs));

    return result;
}

/**
 * Returns a vector of arguments that should replace the given argument after one step of inlining.
 * Note: This function is currently generalised to perform any required inlining within aggregators
 * as well, making it simple to extend to this later on if desired (and the semantic check is removed).
 */
NullableVector<AstArgument*> getInlinedArgument(AstProgram& program, const AstArgument* arg) {
    bool changed = false;
    std::vector<AstArgument*> versions;

    // Each argument has to be handled differently - essentially, want to go down to
    // nested aggregators, and inline their bodies if needed.
    if (const auto* aggr = dynamic_cast<const AstAggregator*>(arg)) {
        // First try inlining the target expression if necessary
        if (aggr->getTargetExpression() != nullptr) {
            NullableVector<AstArgument*> argumentVersions =
                    getInlinedArgument(program, aggr->getTargetExpression());

            if (argumentVersions.isValid()) {
                // An element in the target expression can be inlined!
                changed = true;

                // Create a new aggregator per version of the target expression
                for (AstArgument* newArg : argumentVersions.getVector()) {
                    auto* newAggr = new AstAggregator(aggr->getOperator());
                    newAggr->setTargetExpression(std::unique_ptr<AstArgument>(newArg));
                    for (AstLiteral* lit : aggr->getBodyLiterals()) {
                        newAggr->addBodyLiteral(std::unique_ptr<AstLiteral>(lit->clone()));
                    }
                    versions.push_back(newAggr);
                }
            }
        }

        // Try inlining body arguments if the target expression has not been changed.
        // (At this point we only handle one step of inlining at a time)
        if (!changed) {
            std::vector<AstLiteral*> bodyLiterals = aggr->getBodyLiterals();
            for (size_t i = 0; i < bodyLiterals.size(); i++) {
                AstLiteral* currLit = bodyLiterals[i];

                NullableVector<std::vector<AstLiteral*>> literalVersions =
                        getInlinedLiteral(program, currLit);

                if (literalVersions.isValid()) {
                    // Literal can be inlined!
                    changed = true;

                    AstAggregator::Op op = aggr->getOperator();

                    // Create an aggregator (with the same operation) for each possible body
                    std::vector<AstAggregator*> aggrVersions;
                    for (std::vector<AstLiteral*> inlineVersions : literalVersions.getVector()) {
                        auto* newAggr = new AstAggregator(aggr->getOperator());
                        if (aggr->getTargetExpression() != nullptr) {
                            newAggr->setTargetExpression(
                                    std::unique_ptr<AstArgument>(aggr->getTargetExpression()->clone()));
                        }

                        // Add in everything except the current literal being replaced
                        for (size_t j = 0; j < bodyLiterals.size(); j++) {
                            if (i != j) {
                                newAggr->addBodyLiteral(
                                        std::unique_ptr<AstLiteral>(bodyLiterals[j]->clone()));
                            }
                        }

                        // Add in everything new that replaces that literal
                        for (AstLiteral* addedLit : inlineVersions) {
                            newAggr->addBodyLiteral(std::unique_ptr<AstLiteral>(addedLit));
                        }

                        aggrVersions.push_back(newAggr);
                    }

                    // Create the actual overall aggregator that ties the replacement aggregators together.
                    if (op == AstAggregator::min) {
                        // min x : { a(x) }. <=> min ( min x : { a1(x) }, min x : { a2(x) }, ... )
                        versions.push_back(combineAggregators(aggrVersions, BinaryOp::MIN));
                    } else if (op == AstAggregator::max) {
                        // max x : { a(x) }. <=> max ( max x : { a1(x) }, max x : { a2(x) }, ... )
                        versions.push_back(combineAggregators(aggrVersions, BinaryOp::MAX));
                    } else if (op == AstAggregator::count) {
                        // count : { a(x) }. <=> sum ( count : { a1(x) }, count : { a2(x) }, ... )
                        versions.push_back(combineAggregators(aggrVersions, BinaryOp::ADD));
                    } else if (op == AstAggregator::sum) {
                        // sum x : { a(x) }. <=> sum ( sum x : { a1(x) }, sum x : { a2(x) }, ... )
                        versions.push_back(combineAggregators(aggrVersions, BinaryOp::ADD));
                    } else {
                        assert(false && "Unsupported aggregator type");
                    }
                }

                // Only perform one stage of inlining at a time.
                if (changed) {
                    break;
                }
            }
        }
    } else if (dynamic_cast<const AstFunctor*>(arg)) {
        // Each type of functor (unary, binary, ternary) must be handled differently.
        if (const auto* functor = dynamic_cast<const AstUnaryFunctor*>(arg)) {
            NullableVector<AstArgument*> argumentVersions =
                    getInlinedArgument(program, functor->getOperand());
            if (argumentVersions.isValid()) {
                changed = true;
                for (AstArgument* newArg : argumentVersions.getVector()) {
                    AstArgument* newFunctor =
                            new AstUnaryFunctor(functor->getFunction(), std::unique_ptr<AstArgument>(newArg));
                    versions.push_back(newFunctor);
                }
            }
        } else if (const auto* functor = dynamic_cast<const AstBinaryFunctor*>(arg)) {
            NullableVector<AstArgument*> lhsVersions = getInlinedArgument(program, functor->getLHS());
            if (lhsVersions.isValid()) {
                changed = true;
                for (AstArgument* newLhs : lhsVersions.getVector()) {
                    AstArgument* newFunctor =
                            new AstBinaryFunctor(functor->getFunction(), std::unique_ptr<AstArgument>(newLhs),
                                    std::unique_ptr<AstArgument>(functor->getRHS()->clone()));
                    versions.push_back(newFunctor);
                }
            } else {
                NullableVector<AstArgument*> rhsVersions = getInlinedArgument(program, functor->getRHS());
                if (rhsVersions.isValid()) {
                    changed = true;
                    for (AstArgument* newRhs : rhsVersions.getVector()) {
                        AstArgument* newFunctor = new AstBinaryFunctor(functor->getFunction(),
                                std::unique_ptr<AstArgument>(functor->getLHS()->clone()),
                                std::unique_ptr<AstArgument>(newRhs));
                        versions.push_back(newFunctor);
                    }
                }
            }
        } else if (const auto* functor = dynamic_cast<const AstTernaryFunctor*>(arg)) {
            NullableVector<AstArgument*> leftVersions = getInlinedArgument(program, functor->getArg(0));
            if (leftVersions.isValid()) {
                changed = true;
                for (AstArgument* newLeft : leftVersions.getVector()) {
                    AstArgument* newFunctor = new AstTernaryFunctor(functor->getFunction(),
                            std::unique_ptr<AstArgument>(newLeft),
                            std::unique_ptr<AstArgument>(functor->getArg(1)->clone()),
                            std::unique_ptr<AstArgument>(functor->getArg(2)->clone()));
                    versions.push_back(newFunctor);
                }
            } else {
                NullableVector<AstArgument*> middleVersions = getInlinedArgument(program, functor->getArg(1));
                if (middleVersions.isValid()) {
                    changed = true;
                    for (AstArgument* newMiddle : middleVersions.getVector()) {
                        AstArgument* newFunctor = new AstTernaryFunctor(functor->getFunction(),
                                std::unique_ptr<AstArgument>(functor->getArg(0)->clone()),
                                std::unique_ptr<AstArgument>(newMiddle),
                                std::unique_ptr<AstArgument>(functor->getArg(2)->clone()));
                        versions.push_back(newFunctor);
                    }
                } else {
                    NullableVector<AstArgument*> rightVersions =
                            getInlinedArgument(program, functor->getArg(2));
                    if (rightVersions.isValid()) {
                        changed = true;
                        for (AstArgument* newRight : rightVersions.getVector()) {
                            AstArgument* newFunctor = new AstTernaryFunctor(functor->getFunction(),
                                    std::unique_ptr<AstArgument>(functor->getArg(0)->clone()),
                                    std::unique_ptr<AstArgument>(functor->getArg(1)->clone()),
                                    std::unique_ptr<AstArgument>(newRight));
                            versions.push_back(newFunctor);
                        }
                    }
                }
            }
        }
    } else if (const auto* cast = dynamic_cast<const AstTypeCast*>(arg)) {
        NullableVector<AstArgument*> argumentVersions = getInlinedArgument(program, cast->getValue());
        if (argumentVersions.isValid()) {
            changed = true;
            for (AstArgument* newArg : argumentVersions.getVector()) {
                AstArgument* newTypeCast =
                        new AstTypeCast(std::unique_ptr<AstArgument>(newArg), cast->getType());
                versions.push_back(newTypeCast);
            }
        }
    } else if (const auto* record = dynamic_cast<const AstRecordInit*>(arg)) {
        std::vector<AstArgument*> recordArguments = record->getArguments();
        for (size_t i = 0; i < recordArguments.size(); i++) {
            AstArgument* currentRecArg = recordArguments[i];
            NullableVector<AstArgument*> argumentVersions = getInlinedArgument(program, currentRecArg);
            if (argumentVersions.isValid()) {
                changed = true;
                for (AstArgument* newArgumentVersion : argumentVersions.getVector()) {
                    auto* newRecordArg = new AstRecordInit();
                    for (size_t j = 0; j < recordArguments.size(); j++) {
                        if (i == j) {
                            newRecordArg->add(std::unique_ptr<AstArgument>(newArgumentVersion));
                        } else {
                            newRecordArg->add(std::unique_ptr<AstArgument>(recordArguments[j]->clone()));
                        }
                    }
                    versions.push_back(newRecordArg);
                }
            }

            // Only perform one stage of inlining at a time.
            if (changed) {
                break;
            }
        }
    }

    if (changed) {
        // Return a valid vector - replacements need to be made!
        return NullableVector<AstArgument*>(versions);
    } else {
        // Return an invalid vector - no inlining has occurred
        return NullableVector<AstArgument*>();
    }
}

/**
 * Returns a vector of atoms that should replace the given atom after one step of inlining.
 * Assumes the relation the atom belongs to is not inlined itself.
 */
NullableVector<AstAtom*> getInlinedAtom(AstProgram& program, AstAtom& atom) {
    bool changed = false;
    std::vector<AstAtom*> versions;

    // Try to inline each of the atom's arguments
    std::vector<AstArgument*> arguments = atom.getArguments();
    for (size_t i = 0; i < arguments.size(); i++) {
        AstArgument* arg = arguments[i];

        NullableVector<AstArgument*> argumentVersions = getInlinedArgument(program, arg);

        if (argumentVersions.isValid()) {
            // Argument has replacements
            changed = true;

            // Create a new atom per new version of the argument
            for (AstArgument* newArgument : argumentVersions.getVector()) {
                AstAtom* newAtom = atom.clone();
                newAtom->setArgument(i, std::unique_ptr<AstArgument>(newArgument));
                versions.push_back(newAtom);
            }
        }

        // Only perform one stage of inlining at a time.
        if (changed) {
            break;
        }
    }

    if (changed) {
        // Return a valid vector - replacements need to be made!
        return NullableVector<AstAtom*>(versions);
    } else {
        // Return an invalid vector - no replacements need to be made
        return NullableVector<AstAtom*>();
    }
}

/**
 * Tries to perform a single step of inlining on the given literal.
 * Returns a pair of nullable vectors (v, w) such that:
 *    - v is valid if and only if the literal can be directly inlined, whereby it
 *      contains the bodies that replace it
 *    - if v is not valid, then w is valid if and only if the literal cannot be inlined directly,
 *      but contains a subargument that can be. In this case, it will contain the versions
 *      that will replace it.
 *    - If both are invalid, then no more inlining can occur on this literal and we are done.
 */
NullableVector<std::vector<AstLiteral*>> getInlinedLiteral(AstProgram& program, AstLiteral* lit) {
    bool inlined = false;
    bool changed = false;

    std::vector<std::vector<AstLiteral*>> addedBodyLiterals;
    std::vector<AstLiteral*> versions;

    if (auto* atom = dynamic_cast<AstAtom*>(lit)) {
        // Check if this atom is meant to be inlined
        AstRelation* rel = program.getRelation(atom->getName());

        if (rel->isInline()) {
            // We found an atom in the clause that needs to be inlined!
            // The clause needs to be replaced
            inlined = true;

            // N new clauses should be formed, where N is the number of clauses
            // associated with the inlined relation
            for (AstClause* inClause : rel->getClauses()) {
                // Form the replacement clause
                std::pair<NullableVector<AstLiteral*>, std::vector<AstBinaryConstraint*>> inlineResult =
                        inlineBodyLiterals(atom, inClause);
                NullableVector<AstLiteral*> replacementBodyLiterals = inlineResult.first;
                std::vector<AstBinaryConstraint*> currConstraints = inlineResult.second;

                if (!replacementBodyLiterals.isValid()) {
                    // Failed to unify the atoms! We can skip this one...
                    continue;
                }

                // Unification successful - the returned vector of literals represents one possible body
                // replacement We can add in the unification constraints as part of these literals.
                std::vector<AstLiteral*> bodyResult = replacementBodyLiterals.getVector();

                for (AstBinaryConstraint* cons : currConstraints) {
                    bodyResult.push_back(cons);
                }

                addedBodyLiterals.push_back(bodyResult);
            }
        } else {
            // Not meant to be inlined, but a subargument may be
            NullableVector<AstAtom*> atomVersions = getInlinedAtom(program, *atom);
            if (atomVersions.isValid()) {
                // Subnode needs to be inlined, so we have a vector of replacement atoms
                changed = true;
                for (AstAtom* newAtom : atomVersions.getVector()) {
                    versions.push_back(newAtom);
                }
            }
        }
    } else if (auto* neg = dynamic_cast<AstNegation*>(lit)) {
        // For negations, check the corresponding atom
        AstAtom* atom = neg->getAtom();
        NullableVector<std::vector<AstLiteral*>> atomVersions = getInlinedLiteral(program, atom);

        if (atomVersions.isValid()) {
            // The atom can be inlined
            inlined = true;

            if (atomVersions.getVector().empty()) {
                // No clauses associated with the atom, so just becomes a true literal
                std::vector<AstLiteral*> trueBody;
                // TODO: change this to AstBoolean
                trueBody.push_back(new AstBinaryConstraint(BinaryConstraintOp::EQ,
                        std::make_unique<AstNumberConstant>(1), std::make_unique<AstNumberConstant>(1)));
                addedBodyLiterals.push_back(trueBody);
            } else {
                // Suppose an atom a(x) is inlined and has the following rules:
                //  - a(x) :- a11(x), a12(x).
                //  - a(x) :- a21(x), a22(x).
                // Then, a(x) <- (a11(x) ^ a12(x)) v (a21(x) ^ a22(x))
                //  => !a(x) <- (!a11(x) v !a12(x)) ^ (!a21(x) v !a22(x))
                //  => !a(x) <- (!a11(x) ^ !a21(x)) v (!a11(x) ^ !a22(x)) v ...
                // Essentially, produce every combination (m_1 ^ m_2 ^ ...) where m_i is a
                // negated literal in the ith rule of a.
                addedBodyLiterals = formNegatedLiterals(program, atom);
            }
        }
        for (const auto& curVec : atomVersions.getVector()) {
            for (auto* cur : curVec) {
                delete cur;
            }
        }
    } else if (auto* constraint = dynamic_cast<AstBinaryConstraint*>(lit)) {
        NullableVector<AstArgument*> lhsVersions = getInlinedArgument(program, constraint->getLHS());
        if (lhsVersions.isValid()) {
            changed = true;
            for (AstArgument* newLhs : lhsVersions.getVector()) {
                AstLiteral* newLit = new AstBinaryConstraint(constraint->getOperator(),
                        std::unique_ptr<AstArgument>(newLhs),
                        std::unique_ptr<AstArgument>(constraint->getRHS()->clone()));
                versions.push_back(newLit);
            }
        } else {
            NullableVector<AstArgument*> rhsVersions = getInlinedArgument(program, constraint->getRHS());
            if (rhsVersions.isValid()) {
                changed = true;
                for (AstArgument* newRhs : rhsVersions.getVector()) {
                    AstLiteral* newLit = new AstBinaryConstraint(constraint->getOperator(),
                            std::unique_ptr<AstArgument>(constraint->getLHS()->clone()),
                            std::unique_ptr<AstArgument>(newRhs));
                    versions.push_back(newLit);
                }
            }
        }
    }

    if (changed) {
        // Not inlined directly but found replacement literals
        // Rewrite these as single-literal bodies
        for (AstLiteral* version : versions) {
            std::vector<AstLiteral*> newBody;
            newBody.push_back(version);
            addedBodyLiterals.push_back(newBody);
        }
        inlined = true;
    }

    if (inlined) {
        return NullableVector<std::vector<AstLiteral*>>(addedBodyLiterals);
    } else {
        return NullableVector<std::vector<AstLiteral*>>();
    }
}

/**
 * Returns a list of clauses that should replace the given clause after one step of inlining.
 * If no inlining can occur, the list will only contain a clone of the original clause.
 */
std::vector<AstClause*> getInlinedClause(AstProgram& program, const AstClause& clause) {
    bool changed = false;
    std::vector<AstClause*> versions;

    // Try to inline things contained in the arguments of the head first.
    // E.g. `a(x, max y : { b(y) }) :- c(x).`, where b should be inlined.
    AstAtom* head = clause.getHead();
    NullableVector<AstAtom*> headVersions = getInlinedAtom(program, *head);
    if (headVersions.isValid()) {
        // The head atom can be inlined!
        changed = true;

        // Produce the new clauses with the replacement head atoms
        for (AstAtom* newHead : headVersions.getVector()) {
            auto* newClause = new AstClause();
            newClause->setSrcLoc(clause.getSrcLoc());

            newClause->setHead(std::unique_ptr<AstAtom>(newHead));

            // The body will remain unchanged
            for (AstLiteral* lit : clause.getBodyLiterals()) {
                newClause->addToBody(std::unique_ptr<AstLiteral>(lit->clone()));
            }

            versions.push_back(newClause);
        }
    }

    // Only perform one stage of inlining at a time.
    // If the head atoms did not need inlining, try inlining atoms nested in the body.
    if (!changed) {
        std::vector<AstLiteral*> bodyLiterals = clause.getBodyLiterals();
        for (size_t i = 0; i < bodyLiterals.size(); i++) {
            AstLiteral* currLit = bodyLiterals[i];

            // Three possible cases when trying to inline a literal:
            //  1) The literal itself may be directly inlined. In this case, the atom can be replaced
            //    with multiple different bodies, as the inlined atom may have several rules.
            //  2) Otherwise, the literal itself may not need to be inlined, but a subnode (e.g. an argument)
            //    may need to be inlined. In this case, an altered literal must replace the original.
            //    Again, several possible versions may exist, as the inlined relation may have several rules.
            //  3) The literal does not depend on any inlined relations, and so does not need to be changed.
            NullableVector<std::vector<AstLiteral*>> litVersions = getInlinedLiteral(program, currLit);

            if (litVersions.isValid()) {
                // Case 1 and 2: Inlining has occurred!
                changed = true;

                // The literal may be replaced with several different bodies.
                // Create a new clause for each possible version.
                std::vector<std::vector<AstLiteral*>> bodyVersions = litVersions.getVector();

                // Create the base clause with the current literal removed
                auto baseClause = std::unique_ptr<AstClause>(clause.cloneHead());
                for (AstLiteral* oldLit : bodyLiterals) {
                    if (currLit != oldLit) {
                        baseClause->addToBody(std::unique_ptr<AstLiteral>(oldLit->clone()));
                    }
                }

                for (std::vector<AstLiteral*> body : bodyVersions) {
                    AstClause* replacementClause = baseClause->clone();

                    // Add in the current set of literals replacing the inlined literal
                    // In Case 2, each body contains exactly one literal
                    for (AstLiteral* newLit : body) {
                        replacementClause->addToBody(std::unique_ptr<AstLiteral>(newLit));
                    }

                    versions.push_back(replacementClause);
                }
            }

            // Only replace at most one literal per iteration
            if (changed) {
                break;
            }
        }
    }

    if (!changed) {
        // Case 3: No inlining changes, so a clone of the original should be returned
        std::vector<AstClause*> ret;
        ret.push_back(clause.clone());
        return ret;
    } else {
        // Inlining changes, so return the replacement clauses.
        return versions;
    }
}

bool InlineRelationsTransformer::transform(AstTranslationUnit& translationUnit) {
    bool changed = false;
    AstProgram& program = *translationUnit.getProgram();

    // Replace constants in the head of inlined clauses with (constrained) variables.
    // This is done to simplify atom unification, particularly when negations are involved.
    normaliseInlinedHeads(program);

    // Remove underscores in inlined atoms in the program to avoid issues during atom unification
    nameInlinedUnderscores(program);

    // Keep trying to inline things until we reach a fixed point.
    // Since we know there are no cyclic dependencies between inlined relations, this will necessarily
    // terminate.
    bool clausesChanged = true;
    while (clausesChanged) {
        std::set<AstClause*> clausesToDelete;
        clausesChanged = false;

        // Go through each relation in the program and check if we need to inline any of its clauses
        for (AstRelation* rel : program.getRelations()) {
            // Skip if the relation is going to be inlined
            if (rel->isInline()) {
                continue;
            }

            // Go through the relation's clauses and try inlining them
            for (AstClause* clause : rel->getClauses()) {
                if (containsInlinedAtom(program, *clause)) {
                    // Generate the inlined versions of this clause - the clause will be replaced by these
                    std::vector<AstClause*> newClauses = getInlinedClause(program, *clause);

                    // Replace the clause with these equivalent versions
                    clausesToDelete.insert(clause);
                    for (AstClause* replacementClause : newClauses) {
                        program.appendClause(std::unique_ptr<AstClause>(replacementClause));
                    }

                    // We've changed the program this iteration
                    clausesChanged = true;
                    changed = true;
                }
            }
        }

        // Delete all clauses that were replaced
        for (const AstClause* clause : clausesToDelete) {
            program.removeClause(clause);
        }
    }

    return changed;
}

}  // end of namespace souffle
