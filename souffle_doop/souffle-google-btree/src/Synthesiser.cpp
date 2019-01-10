/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2013, 2015, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file Synthesiser.cpp
 *
 * Implementation of the C++ synthesiser for RAM programs.
 *
 ***********************************************************************/

#include "Synthesiser.h"
#include "BinaryConstraintOps.h"
#include "BinaryFunctorOps.h"
#include "Global.h"
#include "IODirectives.h"
#include "IndexSetAnalysis.h"
#include "LogStatement.h"
#include "RamCondition.h"
#include "RamNode.h"
#include "RamOperation.h"
#include "RamProgram.h"
#include "RamRelation.h"
#include "RamTranslationUnit.h"
#include "RamValue.h"
#include "RamVisitor.h"
#include "SymbolMask.h"
#include "SymbolTable.h"
#include "TernaryFunctorOps.h"
#include "UnaryFunctorOps.h"
#include "Util.h"
#include <algorithm>
#include <cassert>
#include <cctype>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <limits>
#include <typeinfo>
#include <utility>
#include <vector>

namespace souffle {

/** Lookup frequency counter */
unsigned Synthesiser::lookupFreqIdx(const std::string& txt) {
    static unsigned ctr;
    auto pos = idxMap.find(txt);
    if (pos == idxMap.end()) {
        return idxMap[txt] = ctr++;
    } else {
        return idxMap[txt];
    }
}

/** Convert RAM identifier */
const std::string Synthesiser::convertRamIdent(const std::string& name) {
    auto it = identifiers.find(name);
    if (it != identifiers.end()) {
        return it->second;
    }
    // strip leading numbers
    unsigned int i;
    for (i = 0; i < name.length(); ++i) {
        if (isalnum(name.at(i)) || name.at(i) == '_') {
            break;
        }
    }
    std::string id;
    for (auto ch : std::to_string(identifiers.size() + 1) + '_' + name.substr(i)) {
        // alphanumeric characters are allowed
        if (isalnum(ch)) {
            id += ch;
        }
        // all other characters are replaced by an underscore, except when
        // the previous character was an underscore as double underscores
        // in identifiers are reserved by the standard
        else if (id.empty() || id.back() != '_') {
            id += '_';
        }
    }
    // most compilers have a limit of 2048 characters (if they have a limit at all) for
    // identifiers; we use half of that for safety
    id = id.substr(0, 1024);
    identifiers.insert(std::make_pair(name, id));
    return id;
}

/** check whether indexes are disabled */
bool Synthesiser::areIndexesDisabled() {
    bool flag = std::getenv("SOUFFLE_USE_NO_INDEX");
    static bool first = true;
    if (first && flag) {
        std::cout << "WARNING: indexes are ignored!\n";
        first = false;
    }
    return flag;
}

/** Get relation name */
const std::string Synthesiser::getRelationName(const RamRelation& rel) {
    return "rel_" + convertRamIdent(rel.getName());
}

/** Get context name */
const std::string Synthesiser::getOpContextName(const RamRelation& rel) {
    return getRelationName(rel) + "_op_ctxt";
}

/** Get relation type */
std::string Synthesiser::getRelationType(const RamRelation& rel, std::size_t arity, const IndexSet& indexes) {
    std::stringstream res;
    res << "ram::Relation";
    res << "<";

    if (rel.isBTree()) {
        res << "BTree,";
    } else if (rel.isRbtset()) {
        res << "Rbtset,";
    } else if (rel.isHashset()) {
        res << "Hashset,";
    } else if (rel.isBrie()) {
        res << "Brie,";
    } else if (rel.isEqRel()) {
        res << "EqRel,";
    } else {
        auto data_structure = Global::config().get("data-structure");
        if (data_structure == "btree") {
            res << "BTree,";
        } else if (data_structure == "rbtset") {
            res << "Rbtset,";
        } else if (data_structure == "hashset") {
            res << "Hashset,";
        } else if (data_structure == "brie") {
            res << "Brie,";
        } else if (data_structure == "eqrel") {
            res << "Eqrel,";
        } else {
            res << "Auto,";
        }
    }

    res << arity;
    if (!areIndexesDisabled()) {
        for (auto& cur : indexes.getAllOrders()) {
            res << ", ram::index<";
            res << join(cur, ",");
            res << ">";
        }
    }
    res << ">";
    return res.str();
}

/* Convert SearchColums to a template index */
std::string Synthesiser::toIndex(SearchColumns key) {
    std::stringstream tmp;
    tmp << "<";
    int i = 0;
    while (key != 0) {
        if (key % 2) {
            tmp << i;
            if (key > 1) {
                tmp << ",";
            }
        }
        key >>= 1;
        i++;
    }

    tmp << ">";
    return tmp.str();
}

/** Get referenced relations */
std::set<RamRelation> Synthesiser::getReferencedRelations(const RamOperation& op) {
    std::set<RamRelation> res;
    visitDepthFirst(op, [&](const RamNode& node) {
        if (auto scan = dynamic_cast<const RamScan*>(&node)) {
            res.insert(scan->getRelation());
        } else if (auto agg = dynamic_cast<const RamAggregate*>(&node)) {
            res.insert(agg->getRelation());
        } else if (auto notExist = dynamic_cast<const RamNotExists*>(&node)) {
            res.insert(notExist->getRelation());
        } else if (auto project = dynamic_cast<const RamProject*>(&node)) {
            res.insert(project->getRelation());
            if (project->hasFilter()) {
                res.insert(project->getFilter());
            }
        }
    });
    return res;
}

void Synthesiser::emitCode(std::ostream& out, const RamStatement& stmt) {
    class CodeEmitter : public RamVisitor<void, std::ostream&> {
    private:
        Synthesiser& synthesiser;

// macros to add comments to generated code for debugging
#ifndef PRINT_BEGIN_COMMENT
#define PRINT_BEGIN_COMMENT(os)                                                  \
    if (Global::config().has("debug-report") || Global::config().has("verbose")) \
    os << "/* BEGIN " << __FUNCTION__ << " @" << __FILE__ << ":" << __LINE__ << " */\n"
#endif

#ifndef PRINT_END_COMMENT
#define PRINT_END_COMMENT(os)                                                    \
    if (Global::config().has("debug-report") || Global::config().has("verbose")) \
    os << "/* END " << __FUNCTION__ << " @" << __FILE__ << ":" << __LINE__ << " */\n"
#endif

        std::function<void(std::ostream&, const RamNode*)> rec;

    public:
        CodeEmitter(Synthesiser& syn) : synthesiser(syn) {
            rec = [&](std::ostream& out, const RamNode* node) { this->visit(*node, out); };
        }

        // -- relation statements --

        void visitCreate(const RamCreate& /*create*/, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            PRINT_END_COMMENT(out);
        }

        void visitFact(const RamFact& fact, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            out << synthesiser.getRelationName(fact.getRelation()) << "->"
                << "insert(" << join(fact.getValues(), ",", rec) << ");\n";
            PRINT_END_COMMENT(out);
        }

        void visitLoad(const RamLoad& load, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            out << "if (performIO) {\n";
            // get some table details
            for (IODirectives ioDirectives : load.getIODirectives()) {
                out << "try {";
                out << "std::map<std::string, std::string> directiveMap(";
                out << ioDirectives << ");\n";
                out << R"_(if (!inputDirectory.empty() && directiveMap["IO"] == "file" && )_";
                out << "directiveMap[\"filename\"].front() != '/') {";
                out << R"_(directiveMap["filename"] = inputDirectory + "/" + directiveMap["filename"];)_";
                out << "}\n";
                out << "IODirectives ioDirectives(directiveMap);\n";
                out << "IOSystem::getInstance().getReader(";
                out << "SymbolMask({" << load.getRelation().getSymbolMask() << "})";
                out << ", symTable, ioDirectives";
                out << ", " << Global::config().has("provenance");
                out << ")->readAll(*" << synthesiser.getRelationName(load.getRelation());
                out << ");\n";
                out << "} catch (std::exception& e) {std::cerr << \"Error loading data: \" << e.what() << "
                       "'\\n';}\n";
            }
            out << "}\n";
            PRINT_END_COMMENT(out);
        }

        void visitStore(const RamStore& store, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            out << "if (performIO) {\n";
            for (IODirectives ioDirectives : store.getIODirectives()) {
                out << "try {";
                out << "std::map<std::string, std::string> directiveMap(" << ioDirectives << ");\n";
                out << R"_(if (!outputDirectory.empty() && directiveMap["IO"] == "file" && )_";
                out << "directiveMap[\"filename\"].front() != '/') {";
                out << R"_(directiveMap["filename"] = outputDirectory + "/" + directiveMap["filename"];)_";
                out << "}\n";
                out << "IODirectives ioDirectives(directiveMap);\n";
                out << "IOSystem::getInstance().getWriter(";
                out << "SymbolMask({" << store.getRelation().getSymbolMask() << "})";
                out << ", symTable, ioDirectives";
                out << ", " << Global::config().has("provenance");
                out << ")->writeAll(*" << synthesiser.getRelationName(store.getRelation()) << ");\n";
                out << "} catch (std::exception& e) {std::cerr << e.what();exit(1);}\n";
            }
            out << "}\n";
            PRINT_END_COMMENT(out);
        }

        void visitInsert(const RamInsert& insert, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            // enclose operation with a check for an empty relation
            std::set<RamRelation> input_relations;
            visitDepthFirst(insert, [&](const RamScan& scan) { input_relations.insert(scan.getRelation()); });
            if (!input_relations.empty()) {
                out << "if (" << join(input_relations, "&&", [&](std::ostream& out, const RamRelation& rel) {
                    out << "!" << synthesiser.getRelationName(rel) << "->"
                        << "empty()";
                }) << ") ";
            }

            // outline each search operation to improve compilation time
            // Disabled to work around issue #345 with clang 3.7-3.9 & omp.
            // out << "[&]()";

            // enclose operation in its own scope
            out << "{\n";

            // check whether loop nest can be parallelized
            bool parallel = false;
            if (const auto* scan = dynamic_cast<const RamScan*>(&insert.getOperation())) {
                // if this is not a pure existence check
                if (!scan->isPureExistenceCheck()) {
                    // yes it can!
                    parallel = true;

                    const auto& rel = scan->getRelation();
                    const auto& relName = synthesiser.getRelationName(rel);
                    if (scan->getRangeQueryColumns() == 0) {
                        // partition outermost relation
                        out << "auto part = " << relName << "->partition();\n";
                    } else {
                        // check list of keys
                        auto arity = rel.getArity();
                        const auto& rangePattern = scan->getRangePattern();

                        // a lambda for printing boundary key values
                        auto printKeyTuple = [&]() {
                            for (size_t i = 0; i < arity; i++) {
                                if (rangePattern[i] != nullptr) {
                                    visit(rangePattern[i], out);
                                } else {
                                    out << "0";
                                }
                                if (i + 1 < arity) {
                                    out << ",";
                                }
                            }
                        };

                        // get index to be queried
                        auto keys = scan->getRangeQueryColumns();
                        auto index = synthesiser.toIndex(keys);

                        out << "const Tuple<RamDomain," << arity << "> key({{";
                        printKeyTuple();
                        out << "}});\n";
                        out << "auto range = " << relName << "->"
                            << "equalRange" << index << "(key);\n";
                        out << "auto part = range.partition();\n";
                    }

                    // build a parallel block around this loop nest
                    out << "PARALLEL_START;\n";
                }
            }

            // create operation contexts for this operation
            for (const RamRelation& rel : synthesiser.getReferencedRelations(insert.getOperation())) {
                // TODO (#467): this causes bugs for subprogram compilation for record types if artificial
                // dependencies are introduces in the precedence graph
                out << "CREATE_OP_CONTEXT(" << synthesiser.getOpContextName(rel);
                out << "," << synthesiser.getRelationName(rel);
                out << "->createContext());\n";
            }

            visit(insert.getOperation(), out);

            if (parallel) {
                out << "PARALLEL_END;\n";  // end parallel

                // aggregate proof counters
            }

            out << "}\n";  // end lambda
            // out << "();";  // call lambda
            PRINT_END_COMMENT(out);
        }

        void visitMerge(const RamMerge& merge, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            if (merge.getTargetRelation().isEqRel()) {
                out << synthesiser.getRelationName(merge.getSourceRelation()) << "->"
                    << "extend("
                    << "*" << synthesiser.getRelationName(merge.getTargetRelation()) << ");\n";
            }
            out << synthesiser.getRelationName(merge.getTargetRelation()) << "->"
                << "insertAll("
                << "*" << synthesiser.getRelationName(merge.getSourceRelation()) << ");\n";
            PRINT_END_COMMENT(out);
        }

        void visitClear(const RamClear& clear, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            out << synthesiser.getRelationName(clear.getRelation()) << "->"
                << "purge();\n";
            PRINT_END_COMMENT(out);
        }

        void visitDrop(const RamDrop& drop, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);

            out << "if (!isHintsProfilingEnabled() && (performIO || " << drop.getRelation().isTemp() << ")) ";
            out << synthesiser.getRelationName(drop.getRelation()) << "->"
                << "purge();\n";

            PRINT_END_COMMENT(out);
        }

        void visitPrintSize(const RamPrintSize& print, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            out << "if (performIO) {\n";
            out << "{ auto lease = getOutputLock().acquire(); \n";
            out << "(void)lease;\n";
            out << "std::cout << R\"(" << print.getMessage() << ")\" <<  ";
            out << synthesiser.getRelationName(print.getRelation()) << "->"
                << "size() << std::endl;\n";
            out << "}";
            out << "}\n";
            PRINT_END_COMMENT(out);
        }

        void visitLogSize(const RamLogSize& print, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            out << "ProfileEventSingleton::instance().makeQuantityEvent( R\"(";
            out << print.getMessage() << ")\",";
            out << synthesiser.getRelationName(print.getRelation()) << "->size(),iter);";
            PRINT_END_COMMENT(out);
        }

        // -- control flow statements --

        void visitSequence(const RamSequence& seq, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            for (const auto& cur : seq.getStatements()) {
                visit(cur, out);
            }
            PRINT_END_COMMENT(out);
        }

        void visitParallel(const RamParallel& parallel, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            auto stmts = parallel.getStatements();

            // special handling cases
            if (stmts.empty()) {
                PRINT_END_COMMENT(out);
                return;
            }

            // a single statement => save the overhead
            if (stmts.size() == 1) {
                visit(stmts[0], out);
                PRINT_END_COMMENT(out);
                return;
            }

            // more than one => parallel sections

            // start parallel section
            out << "SECTIONS_START;\n";

            // put each thread in another section
            for (const auto& cur : stmts) {
                out << "SECTION_START;\n";
                visit(cur, out);
                out << "SECTION_END\n";
            }

            // done
            out << "SECTIONS_END;\n";
            PRINT_END_COMMENT(out);
        }

        void visitLoop(const RamLoop& loop, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            out << "iter = 0;\n";
            out << "for(;;) {\n";
            visit(loop.getBody(), out);
            out << "iter++;\n";
            out << "}\n";
            out << "iter = 0;\n";
            PRINT_END_COMMENT(out);
        }

        void visitSwap(const RamSwap& swap, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            const std::string tempKnowledge = "rel_0";
            const std::string& deltaKnowledge = synthesiser.getRelationName(swap.getFirstRelation());
            const std::string& newKnowledge = synthesiser.getRelationName(swap.getSecondRelation());

            // perform a triangular swap of pointers
            out << "{\nauto " << tempKnowledge << " = " << deltaKnowledge << ";\n"
                << deltaKnowledge << " = " << newKnowledge << ";\n"
                << newKnowledge << " = " << tempKnowledge << ";\n"
                << "}\n";
            PRINT_END_COMMENT(out);
        }

        void visitExit(const RamExit& exit, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            out << "if(";
            visit(exit.getCondition(), out);
            out << ") break;\n";
            PRINT_END_COMMENT(out);
        }

        void visitLogTimer(const RamLogTimer& timer, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            // create local scope for name resolution
            out << "{\n";

            const std::string ext = fileExtension(Global::config().get("profile"));

            // create local timer
            out << "\tLogger logger(R\"_(" << timer.getMessage() << ")_\",iter);\n";

            // insert statement to be measured
            visit(timer.getStatement(), out);

            // done
            out << "}\n";
            PRINT_END_COMMENT(out);
        }

        void visitDebugInfo(const RamDebugInfo& dbg, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            out << "SignalHandler::instance()->setMsg(R\"_(";
            out << dbg.getMessage();
            out << ")_\");\n";

            // insert statements of the rule
            visit(dbg.getStatement(), out);
            PRINT_END_COMMENT(out);
        }

        void visitStratum(const RamStratum& stratum, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            PRINT_END_COMMENT(out);
        }

        // -- operations --

        void visitSearch(const RamSearch& search, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            auto condition = search.getCondition();
            if (condition) {
                out << "if( ";
                visit(condition, out);
                out << ") {\n";
                visit(search.getNestedOperation(), out);
                if (Global::config().has("profile") && !search.getProfileText().empty()) {
                    out << "freqs[" << synthesiser.lookupFreqIdx(search.getProfileText()) << "]++;\n";
                }
                out << "}\n";
            } else {
                visit(search.getNestedOperation(), out);
                if (Global::config().has("profile") && !search.getProfileText().empty()) {
                    out << "freqs[" << synthesiser.lookupFreqIdx(search.getProfileText()) << "]++;\n";
                }
            }
            PRINT_END_COMMENT(out);
        }

        void visitScan(const RamScan& scan, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            // get relation name
            const auto& rel = scan.getRelation();
            auto relName = synthesiser.getRelationName(rel);
            auto ctxName = "READ_OP_CONTEXT(" + synthesiser.getOpContextName(rel) + ")";
            auto level = scan.getLevel();

            // if this search is a full scan
            if (scan.getRangeQueryColumns() == 0) {
                if (scan.isPureExistenceCheck()) {
                    out << "if(!" << relName << "->"
                        << "empty()) {\n";
                    visitSearch(scan, out);
                    out << "}\n";
                } else if (scan.getLevel() == 0) {
                    // make this loop parallel
                    // partition outermost relation
                    out << "pfor(auto it = part.begin(); it<part.end(); ++it) \n";
                    out << "try{";
                    out << "for(const auto& env0 : *it) {\n";
                    visitSearch(scan, out);
                    out << "}\n";
                    out << "} catch(std::exception &e) { SignalHandler::instance()->error(e.what());}\n";
                } else {
                    out << "for(const auto& env" << level << " : "
                        << "*" << relName << ") {\n";
                    visitSearch(scan, out);
                    out << "}\n";
                }
                return;
                PRINT_END_COMMENT(out);
            }

            // check list of keys
            auto arity = rel.getArity();
            const auto& rangePattern = scan.getRangePattern();

            // a lambda for printing boundary key values
            auto printKeyTuple = [&]() {
                for (size_t i = 0; i < arity; i++) {
                    if (rangePattern[i] != nullptr) {
                        visit(rangePattern[i], out);
                    } else {
                        out << "0";
                    }
                    if (i + 1 < arity) {
                        out << ",";
                    }
                }
            };

            // get index to be queried
            auto keys = scan.getRangeQueryColumns();
            auto index = synthesiser.toIndex(keys);

            // if this is the parallel level
            if (scan.getLevel() == 0 && !scan.isPureExistenceCheck()) {
                // make this loop parallel
                out << "pfor(auto it = part.begin(); it<part.end(); ++it) { \n";
                out << "try{";
                out << "for(const auto& env0 : *it) {\n";
                visitSearch(scan, out);
                out << "}\n";
                out << "} catch(std::exception &e) { SignalHandler::instance()->error(e.what());}\n";
                out << "}\n";
                return;
                PRINT_END_COMMENT(out);
            }

            // if it is a equality-range query
            out << "const Tuple<RamDomain," << arity << "> key({{";
            printKeyTuple();
            out << "}});\n";
            out << "auto range = " << relName << "->"
                << "equalRange" << index << "(key," << ctxName << ");\n";
            if (scan.isPureExistenceCheck()) {
                out << "if(!range.empty()) {\n";
                visitSearch(scan, out);
                out << "}\n";
            } else {
                out << "for(const auto& env" << level << " : range) {\n";
                visitSearch(scan, out);
                out << "}\n";
            }
            PRINT_END_COMMENT(out);
        }

        void visitLookup(const RamLookup& lookup, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            auto arity = lookup.getArity();

            // get the tuple type working with
            std::string tuple_type = "ram::Tuple<RamDomain," + toString(arity) + ">";

            // look up reference
            out << "auto ref = env" << lookup.getReferenceLevel() << "[" << lookup.getReferencePosition()
                << "];\n";
            out << "if (isNull<" << tuple_type << ">(ref)) continue;\n";
            out << tuple_type << " env" << lookup.getLevel() << " = unpack<" << tuple_type << ">(ref);\n";

            out << "{\n";

            // continue with condition checks and nested body
            visitSearch(lookup, out);

            out << "}\n";
            PRINT_END_COMMENT(out);
        }

        void visitAggregate(const RamAggregate& aggregate, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            // get some properties
            const auto& rel = aggregate.getRelation();
            auto arity = rel.getArity();
            auto relName = synthesiser.getRelationName(rel);
            auto ctxName = "READ_OP_CONTEXT(" + synthesiser.getOpContextName(rel) + ")";
            auto level = aggregate.getLevel();

            // get the tuple type working with
            std::string tuple_type = "ram::Tuple<RamDomain," + toString(std::max(1u, arity)) + ">";

            // declare environment variable
            out << tuple_type << " env" << level << ";\n";

            // special case: counting of number elements in a full relation
            if (aggregate.getFunction() == RamAggregate::COUNT && aggregate.getRangeQueryColumns() == 0) {
                // shortcut: use relation size
                out << "env" << level << "[0] = " << relName << "->"
                    << "size();\n";
                visitSearch(aggregate, out);
                PRINT_END_COMMENT(out);
                return;
            }

            // init result
            std::string init;
            switch (aggregate.getFunction()) {
                case RamAggregate::MIN:
                    init = "MAX_RAM_DOMAIN";
                    break;
                case RamAggregate::MAX:
                    init = "MIN_RAM_DOMAIN";
                    break;
                case RamAggregate::COUNT:
                    init = "0";
                    break;
                case RamAggregate::SUM:
                    init = "0";
                    break;
            }
            out << "RamDomain res = " << init << ";\n";

            // get range to aggregate
            auto keys = aggregate.getRangeQueryColumns();

            // check whether there is an index to use
            if (keys == 0) {
                // no index => use full relation
                out << "auto& range = "
                    << "*" << relName << ";\n";
            } else {
                // a lambda for printing boundary key values
                auto printKeyTuple = [&]() {
                    for (size_t i = 0; i < arity; i++) {
                        if (aggregate.getPattern()[i] != nullptr) {
                            visit(aggregate.getPattern()[i], out);
                        } else {
                            out << "0";
                        }
                        if (i + 1 < arity) {
                            out << ",";
                        }
                    }
                };

                // get index
                auto index = synthesiser.toIndex(keys);
                out << "const " << tuple_type << " key({{";
                printKeyTuple();
                out << "}});\n";
                out << "auto range = " << relName << "->"
                    << "equalRange" << index << "(key," << ctxName << ");\n";
            }

            // add existence check
            if (aggregate.getFunction() != RamAggregate::COUNT) {
                out << "if(!range.empty()) {\n";
            }

            // aggregate result
            out << "for(const auto& cur : range) {\n";

            // create aggregation code
            if (aggregate.getFunction() == RamAggregate::COUNT) {
                // count is easy
                out << "++res\n;";
            } else if (aggregate.getFunction() == RamAggregate::SUM) {
                out << "env" << level << " = cur;\n";
                out << "res += ";
                visit(*aggregate.getTargetExpression(), out);
                out << ";\n";
            } else {
                // pick function
                std::string fun = "min";
                switch (aggregate.getFunction()) {
                    case RamAggregate::MIN:
                        fun = "std::min";
                        break;
                    case RamAggregate::MAX:
                        fun = "std::max";
                        break;
                    case RamAggregate::COUNT:
                        assert(false);
                    case RamAggregate::SUM:
                        assert(false);
                }

                out << "env" << level << " = cur;\n";
                out << "res = " << fun << "(res,";
                visit(*aggregate.getTargetExpression(), out);
                out << ");\n";
            }

            // end aggregator loop
            out << "}\n";

            // write result into environment tuple
            out << "env" << level << "[0] = res;\n";

            // continue with condition checks and nested body
            out << "{\n";

            auto condition = aggregate.getCondition();
            if (condition) {
                out << "if( ";
                visit(condition, out);
                out << ") {\n";
                visitSearch(aggregate, out);
                out << "}\n";
            } else {
                visitSearch(aggregate, out);
            }

            out << "}\n";

            // end conditional nested block
            if (aggregate.getFunction() != RamAggregate::COUNT) {
                out << "}\n";
            }
            PRINT_END_COMMENT(out);
        }

        void visitProject(const RamProject& project, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            const auto& rel = project.getRelation();
            auto arity = rel.getArity();
            auto relName = synthesiser.getRelationName(rel);
            auto ctxName = "READ_OP_CONTEXT(" + synthesiser.getOpContextName(rel) + ")";

            // check condition
            auto condition = project.getCondition();
            if (condition) {
                out << "if (";
                visit(condition, out);
                out << ") {\n";
            }

            // create projected tuple
            if (project.getValues().empty()) {
                out << "Tuple<RamDomain," << arity << "> tuple({{}});\n";
            } else {
                out << "Tuple<RamDomain," << arity << "> tuple({{(RamDomain)("
                    << join(project.getValues(), "),(RamDomain)(", rec) << ")}});\n";

                // check filter
            }
            if (project.hasFilter()) {
                auto relFilter = synthesiser.getRelationName(project.getFilter());
                auto ctxFilter = "READ_OP_CONTEXT(" + synthesiser.getOpContextName(project.getFilter()) + ")";
                out << "if (!" << relFilter << ".contains(tuple," << ctxFilter << ")) {";
            }

            // insert tuple
            out << relName << "->"
                << "insert(tuple," << ctxName << ");\n";

            // end filter
            if (project.hasFilter()) {
                out << "}";
            }

            // end condition
            if (condition) {
                out << "}\n";
            }
            PRINT_END_COMMENT(out);
        }

        // -- conditions --

        void visitAnd(const RamAnd& c, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            out << "((";
            visit(c.getLHS(), out);
            out << ") && (";
            visit(c.getRHS(), out);
            out << "))";
            PRINT_END_COMMENT(out);
        }

        void visitBinaryRelation(const RamBinaryRelation& rel, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            switch (rel.getOperator()) {
                // comparison operators
                case BinaryConstraintOp::EQ:
                    out << "((";
                    visit(rel.getLHS(), out);
                    out << ") == (";
                    visit(rel.getRHS(), out);
                    out << "))";
                    break;
                case BinaryConstraintOp::NE:
                    out << "((";
                    visit(rel.getLHS(), out);
                    out << ") != (";
                    visit(rel.getRHS(), out);
                    out << "))";
                    break;
                case BinaryConstraintOp::LT:
                    out << "((";
                    visit(rel.getLHS(), out);
                    out << ") < (";
                    visit(rel.getRHS(), out);
                    out << "))";
                    break;
                case BinaryConstraintOp::LE:
                    out << "((";
                    visit(rel.getLHS(), out);
                    out << ") <= (";
                    visit(rel.getRHS(), out);
                    out << "))";
                    break;
                case BinaryConstraintOp::GT:
                    out << "((";
                    visit(rel.getLHS(), out);
                    out << ") > (";
                    visit(rel.getRHS(), out);
                    out << "))";
                    break;
                case BinaryConstraintOp::GE:
                    out << "((";
                    visit(rel.getLHS(), out);
                    out << ") >= (";
                    visit(rel.getRHS(), out);
                    out << "))";
                    break;

                // strings
                case BinaryConstraintOp::MATCH: {
                    out << "regex_wrapper(symTable.resolve(";
                    visit(rel.getLHS(), out);
                    out << "),symTable.resolve(";
                    visit(rel.getRHS(), out);
                    out << "))";
                    break;
                }
                case BinaryConstraintOp::NOT_MATCH: {
                    out << "!regex_wrapper(symTable.resolve(";
                    visit(rel.getLHS(), out);
                    out << "),symTable.resolve(";
                    visit(rel.getRHS(), out);
                    out << "))";
                    break;
                }
                case BinaryConstraintOp::CONTAINS: {
                    out << "(symTable.resolve(";
                    visit(rel.getRHS(), out);
                    out << ").find(symTable.resolve(";
                    visit(rel.getLHS(), out);
                    out << ")) != std::string::npos)";
                    break;
                }
                case BinaryConstraintOp::NOT_CONTAINS: {
                    out << "(symTable.resolve(";
                    visit(rel.getRHS(), out);
                    out << ").find(symTable.resolve(";
                    visit(rel.getLHS(), out);
                    out << ")) == std::string::npos)";
                    break;
                }
                default:
                    assert(false && "Unsupported Operation!");
                    break;
            }
            PRINT_END_COMMENT(out);
        }

        void visitEmpty(const RamEmpty& empty, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            out << synthesiser.getRelationName(empty.getRelation()) << "->"
                << "empty()";
            PRINT_END_COMMENT(out);
        }

        void visitNotExists(const RamNotExists& ne, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            // get some details
            const auto& rel = ne.getRelation();
            auto relName = synthesiser.getRelationName(rel);
            auto ctxName = "READ_OP_CONTEXT(" + synthesiser.getOpContextName(rel) + ")";
            auto arity = rel.getArity();

            // if it is total we use the contains function
            if (ne.isTotal()) {
                out << "!" << relName << "->"
                    << "contains(Tuple<RamDomain," << arity << ">({{" << join(ne.getValues(), ",", rec)
                    << "}})," << ctxName << ")";
                return;
                PRINT_END_COMMENT(out);
            }

            // else we conduct a range query
            out << relName << "->"
                << "equalRange";
            out << synthesiser.toIndex(ne.getKey());
            out << "(Tuple<RamDomain," << arity << ">({{";
            out << join(ne.getValues(), ",", [&](std::ostream& out, RamValue* value) {
                if (!value) {
                    out << "0";
                } else {
                    visit(*value, out);
                }
            });
            out << "}})," << ctxName << ").empty()";
            PRINT_END_COMMENT(out);
        }

        // -- values --
        void visitNumber(const RamNumber& num, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            out << "RamDomain(" << num.getConstant() << ")";
            PRINT_END_COMMENT(out);
        }

        void visitElementAccess(const RamElementAccess& access, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            out << "env" << access.getLevel() << "[" << access.getElement() << "]";
            PRINT_END_COMMENT(out);
        }

        void visitAutoIncrement(const RamAutoIncrement& /*inc*/, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            out << "(ctr++)";
            PRINT_END_COMMENT(out);
        }

        void visitUnaryOperator(const RamUnaryOperator& op, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            switch (op.getOperator()) {
                case UnaryOp::ORD:
                    visit(op.getValue(), out);
                    break;
                case UnaryOp::STRLEN:
                    out << "symTable.resolve((size_t)";
                    visit(op.getValue(), out);
                    out << ").size()";
                    break;
                case UnaryOp::NEG:
                    out << "(-(";
                    visit(op.getValue(), out);
                    out << "))";
                    break;
                case UnaryOp::BNOT:
                    out << "(~(";
                    visit(op.getValue(), out);
                    out << "))";
                    break;
                case UnaryOp::LNOT:
                    out << "(!(";
                    visit(op.getValue(), out);
                    out << "))";
                    break;
                case UnaryOp::TOSTRING:
                    out << "symTable.lookup(std::to_string(";
                    visit(op.getValue(), out);
                    out << "))";
                    break;
                case UnaryOp::TONUMBER:
                    out << "(wrapper_tonumber(symTable.resolve((size_t)";
                    visit(op.getValue(), out);
                    out << ")))";
                    break;

                default:
                    assert(false && "Unsupported Operation!");
                    break;
            }
            PRINT_END_COMMENT(out);
        }

        void visitBinaryOperator(const RamBinaryOperator& op, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            switch (op.getOperator()) {
                // arithmetic
                case BinaryOp::ADD: {
                    out << "(";
                    visit(op.getLHS(), out);
                    out << ") + (";
                    visit(op.getRHS(), out);
                    out << ")";
                    break;
                }
                case BinaryOp::SUB: {
                    out << "(";
                    visit(op.getLHS(), out);
                    out << ") - (";
                    visit(op.getRHS(), out);
                    out << ")";
                    break;
                }
                case BinaryOp::MUL: {
                    out << "(";
                    visit(op.getLHS(), out);
                    out << ") * (";
                    visit(op.getRHS(), out);
                    out << ")";
                    break;
                }
                case BinaryOp::DIV: {
                    out << "(";
                    visit(op.getLHS(), out);
                    out << ") / (";
                    visit(op.getRHS(), out);
                    out << ")";
                    break;
                }
                case BinaryOp::EXP: {
                    out << "(AstDomain)(std::pow((AstDomain)";
                    visit(op.getLHS(), out);
                    out << ",(AstDomain)";
                    visit(op.getRHS(), out);
                    out << "))";
                    break;
                }
                case BinaryOp::MOD: {
                    out << "(";
                    visit(op.getLHS(), out);
                    out << ") % (";
                    visit(op.getRHS(), out);
                    out << ")";
                    break;
                }
                case BinaryOp::BAND: {
                    out << "(";
                    visit(op.getLHS(), out);
                    out << ") & (";
                    visit(op.getRHS(), out);
                    out << ")";
                    break;
                }
                case BinaryOp::BOR: {
                    out << "(";
                    visit(op.getLHS(), out);
                    out << ") | (";
                    visit(op.getRHS(), out);
                    out << ")";
                    break;
                }
                case BinaryOp::BXOR: {
                    out << "(";
                    visit(op.getLHS(), out);
                    out << ") ^ (";
                    visit(op.getRHS(), out);
                    out << ")";
                    break;
                }
                case BinaryOp::LAND: {
                    out << "(";
                    visit(op.getLHS(), out);
                    out << ") && (";
                    visit(op.getRHS(), out);
                    out << ")";
                    break;
                }
                case BinaryOp::LOR: {
                    out << "(";
                    visit(op.getLHS(), out);
                    out << ") || (";
                    visit(op.getRHS(), out);
                    out << ")";
                    break;
                }
                case BinaryOp::MAX: {
                    out << "(AstDomain)(std::max((AstDomain)";
                    visit(op.getLHS(), out);
                    out << ",(AstDomain)";
                    visit(op.getRHS(), out);
                    out << "))";
                    break;
                }
                case BinaryOp::MIN: {
                    out << "(AstDomain)(std::min((AstDomain)";
                    visit(op.getLHS(), out);
                    out << ",(AstDomain)";
                    visit(op.getRHS(), out);
                    out << "))";
                    break;
                }

                // strings
                case BinaryOp::CAT: {
                    out << "symTable.lookup(";
                    out << "symTable.resolve(";
                    visit(op.getLHS(), out);
                    out << ") + symTable.resolve(";
                    visit(op.getRHS(), out);
                    out << "))";
                    break;
                }
                default:
                    assert(false && "Unsupported Operation!");
            }
            PRINT_END_COMMENT(out);
        }

        void visitTernaryOperator(const RamTernaryOperator& op, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            switch (op.getOperator()) {
                case TernaryOp::SUBSTR:
                    out << "symTable.lookup(";
                    out << "substr_wrapper(symTable.resolve(";
                    visit(op.getArg(0), out);
                    out << "),(";
                    visit(op.getArg(1), out);
                    out << "),(";
                    visit(op.getArg(2), out);
                    out << ")))";
                    break;
                default:
                    assert(false && "Unsupported Operation!");
            }
            PRINT_END_COMMENT(out);
        }

        // -- records --

        void visitPack(const RamPack& pack, std::ostream& out) override {
            PRINT_BEGIN_COMMENT(out);
            out << "pack("
                << "ram::Tuple<RamDomain," << pack.getValues().size() << ">({"
                << join(pack.getValues(), ",", rec) << "})"
                << ")";
            PRINT_END_COMMENT(out);
        }

        // -- subroutine argument --

        void visitArgument(const RamArgument& arg, std::ostream& out) override {
            out << "(args)[" << arg.getArgNumber() << "]";
        }

        // -- subroutine return --

        void visitReturn(const RamReturn& ret, std::ostream& out) override {
            for (auto val : ret.getValues()) {
                if (val == nullptr) {
                    out << "ret.push_back(0);\n";
                    out << "err.push_back(true);\n";
                } else {
                    out << "ret.push_back(";
                    visit(val, out);
                    out << ");\n";
                    out << "err.push_back(false);\n";
                }
            }
        }

#ifdef USE_MPI

        // -- mpi statements --

        void visitRecv(const RamRecv& recv, std::ostream& os) override {
            os << "\n#ifdef USE_MPI\n";
            os << "{";
            os << "auto status = souffle::mpi::probe(";
            // source
            os << recv.getSourceStratum() + 1 << ", ";
            // tag
            os << "tag_" << synthesiser.getRelationName(recv.getRelation());
            os << ");";
            os << "souffle::mpi::recv<RamDomain>(";
            // data
            os << "*" << synthesiser.getRelationName(recv.getRelation()) << ", ";
            // arity
            os << recv.getRelation().getArity() << ", ";
            // status
            os << "status";
            os << ");";
            os << "}";
            os << "\n#endif\n";
        }

        void visitSend(const RamSend& send, std::ostream& os) override {
            os << "\n#ifdef USE_MPI\n";
            os << "{";
            os << "souffle::mpi::send<RamDomain>(";
            // data
            os << "*" << synthesiser.getRelationName(send.getRelation()) << ", ";
            // arity
            os << send.getRelation().getArity() << ", ";
            // destinations
            const auto& destinationStrata = send.getDestinationStrata();
            auto it = destinationStrata.begin();
            os << "std::set<int>(";
            if (it != destinationStrata.end()) {
                os << "{" << *it + 1;
                ++it;
                while (it != destinationStrata.end()) {
                    os << ", " << *it + 1;
                    ++it;
                }
                os << "}";
            } else {
                os << "0";
            }
            os << "), ";
            // tag
            os << "tag_" << synthesiser.getRelationName(send.getRelation());
            os << ");";
            os << "}";
            os << "\n#endif\n";
        }

        void visitNotify(const RamNotify&, std::ostream& os) override {
            os << "\n#ifdef USE_MPI\n";
            os << "mpi::send(0, SymbolTable::exitTag());";
            os << "mpi::recv(0, SymbolTable::exitTag());";
            os << "\n#endif\n";
        }

        void visitWait(const RamWait& wait, std::ostream& os) override {
            os << "\n#ifdef USE_MPI\n";
            os << "symTable.handleMpiMessages(" << wait.getCount() << ");";
            os << "\n#endif\n";
        }

#endif
        // -- safety net --

        void visitNode(const RamNode& node, std::ostream& /*out*/) override {
            std::cerr << "Unsupported node type: " << typeid(node).name() << "\n";
            assert(false && "Unsupported Node Type!");
        }
    };

    // emit code
    CodeEmitter(*this).visit(stmt, out);
}

void Synthesiser::generateCode(const RamTranslationUnit& unit, std::ostream& os, const std::string& id) {
    // ---------------------------------------------------------------
    //                      Auto-Index Generation
    // ---------------------------------------------------------------
    const SymbolTable& symTable = unit.getSymbolTable();
    const RamProgram& prog = unit.getP();
    auto* idxAnalysis = unit.getAnalysis<IndexSetAnalysis>();

    // ---------------------------------------------------------------
    //                      Code Generation
    // ---------------------------------------------------------------

    std::string classname = "Sf_" + id;

#ifdef USE_MPI
    // turn off mpi support if not enabled as the execution engine
    if (Global::config().get("engine") != "mpi") {
        os << "#undef USE_MPI\n";
    }
#endif

    // generate C++ program
    os << "\n#include \"souffle/CompiledSouffle.h\"\n";
    if (Global::config().has("provenance")) {
        os << "\n#include \"souffle/Explain.h\"\n";
    }

    if (Global::config().has("live-profile")) {
        os << "#include <thread>\n";
        os << "#include \"souffle/profile/Tui.h\"\n";
    }

    os << "\n";
    os << "namespace souffle {\n";
    os << "using namespace ram;\n";

    os << "class " << classname << " : public SouffleProgram {\n";

    // regex wrapper
    os << "private:\n";
    os << "static inline bool regex_wrapper(const std::string& pattern, const std::string& text) {\n";
    os << "   bool result = false; \n";
    os << "   try { result = std::regex_match(text, std::regex(pattern)); } catch(...) { \n";
    os << "     std::cerr << \"warning: wrong pattern provided for match(\\\"\" << pattern << \"\\\",\\\"\" "
          "<< text << \"\\\").\\n\";\n}\n";
    os << "   return result;\n";
    os << "}\n";

    // substring wrapper
    os << "private:\n";
    os << "static inline std::string substr_wrapper(const std::string& str, size_t idx, size_t len) {\n";
    os << "   std::string result; \n";
    os << "   try { result = str.substr(idx,len); } catch(...) { \n";
    os << "     std::cerr << \"warning: wrong index position provided by substr(\\\"\";\n";
    os << "     std::cerr << str << \"\\\",\" << (int32_t)idx << \",\" << (int32_t)len << \") "
          "functor.\\n\";\n";
    os << "   } return result;\n";
    os << "}\n";

    // to number wrapper
    os << "private:\n";
    os << "static inline RamDomain wrapper_tonumber(const std::string& str) {\n";
    os << "   RamDomain result=0; \n";
    os << "   try { result = stord(str); } catch(...) { \n";
    os << "     std::cerr << \"error: wrong string provided by to_number(\\\"\";\n";
    os << "     std::cerr << str << \"\\\") ";
    os << "functor.\\n\";\n";
    os << "     raise(SIGFPE);\n";
    os << "   } return result;\n";
    os << "}\n";

// if using mpi...
#ifdef USE_MPI
    if (Global::config().get("engine") == "mpi") {
        os << "\n#ifdef USE_MPI\n";

        // create an enum of message tags, one for each relation
        {
            os << "private:\n";
            os << "enum {";
            {
                int tag = SymbolTable::numberOfTags();
                visitDepthFirst(*(prog.getMain()), [&](const RamCreate& create) {
                    if (tag != SymbolTable::numberOfTags()) {
                        os << ", ";
                    }
                    os << "tag_" << getRelationName(create.getRelation()) << " = " << tag;
                    ++tag;
                });
            }
            os << "};";
        }
        os << "\n#endif\n";
    }
#endif

    if (Global::config().has("profile")) {
        os << "std::string profiling_fname;\n";
    }

    os << "public:\n";

    // declare symbol table
    os << "// -- initialize symbol table --\n";
    {
        os << "SymbolTable symTable\n";
        if (symTable.size() > 0) {
            os << "{\n";
            for (size_t i = 0; i < symTable.size(); i++) {
                os << "\tR\"_(" << symTable.resolve(i) << ")_\",\n";
            }
            os << "}";
        }
        os << ";";
    }

    if (Global::config().has("profile")) {
        os << "private:\n";
        size_t numFreq = 0;
        visitDepthFirst(*(prog.getMain()), [&](const RamStatement& node) { numFreq++; });
        os << "  size_t freqs[" << numFreq << "]{};\n";
    }

    // print relation definitions
    std::string initCons;      // initialization of constructor
    std::string deleteForNew;  // matching deletes for each new, used in the destructor
    std::string registerRel;   // registration of relations
    int relCtr = 0;
    std::string tempType;  // string to hold the type of the temporary relations
    visitDepthFirst(*(prog.getMain()), [&](const RamCreate& create) {
        // get some table details
        const auto& rel = create.getRelation();
        int arity = rel.getArity();
        const std::string& raw_name = rel.getName();
        const std::string& name = getRelationName(rel);

        // ensure that the type of the new knowledge is the same as that of the delta knowledge
        tempType = (rel.isTemp() && raw_name.find("@delta") != std::string::npos)
                           ? getRelationType(rel, rel.getArity(), idxAnalysis->getIndexes(rel))
                           : tempType;
        const std::string& type = (rel.isTemp()) ? tempType : getRelationType(rel, rel.getArity(),
                                                                      idxAnalysis->getIndexes(rel));

        // defining table
        os << "// -- Table: " << raw_name << "\n";
        os << type << "* " << name << ";\n";
        if (!initCons.empty()) {
            initCons += ",\n";
        }
        initCons += name + "(new " + type + "())";
        deleteForNew += "delete " + name + ";\n";
        if ((rel.isInput() || rel.isComputed() || Global::config().has("provenance")) && !rel.isTemp()) {
            os << "souffle::RelationWrapper<";
            os << relCtr++ << ",";
            os << type << ",";
            os << "Tuple<RamDomain," << arity << ">,";
            os << arity << ",";
            os << (rel.isInput() ? "true" : "false") << ",";
            os << (rel.isComputed() ? "true" : "false");
            os << "> wrapper_" << name << ";\n";

            // construct types
            std::string tupleType = "std::array<const char *," + std::to_string(arity) + ">{{";
            std::string tupleName = "std::array<const char *," + std::to_string(arity) + ">{{";

            if (rel.getArity()) {
                tupleType += "\"" + rel.getArgTypeQualifier(0) + "\"";
                for (int i = 1; i < arity; i++) {
                    tupleType += ",\"" + rel.getArgTypeQualifier(i) + "\"";
                }

                tupleName += "\"" + rel.getArg(0) + "\"";
                for (int i = 1; i < arity; i++) {
                    tupleName += ",\"" + rel.getArg(i) + "\"";
                }
            }
            tupleType += "}}";
            tupleName += "}}";

            initCons += ",\nwrapper_" + name + "(" + "*" + name + ",symTable,\"" + raw_name + "\"," +
                        tupleType + "," + tupleName + ")";
            registerRel += "addRelation(\"" + raw_name + "\",&wrapper_" + name + "," +
                           std::to_string(rel.isInput()) + "," + std::to_string(rel.isOutput()) + ");\n";
        }
    });

    os << "public:\n";

    // -- constructor --

    os << classname;
    if (Global::config().has("profile")) {
        os << "(std::string pf=\"profile.log\") : profiling_fname(pf)";
        if (!initCons.empty()) {
            os << ",\n" << initCons;
        }
    } else {
        os << "()";
        if (!initCons.empty()) {
            os << " : " << initCons;
        }
    }
    os << "{\n";
    os << registerRel;
    os << "}\n";
    // -- destructor --

    os << "~" << classname << "() {\n";
    os << deleteForNew;
    os << "}\n";

    // -- run function --
    os << "private:\ntemplate <bool performIO> void runFunction(std::string inputDirectory = \".\", "
          "std::string outputDirectory = \".\", size_t stratumIndex = (size_t) -1) {\n";

    os << "SignalHandler::instance()->set();\n";
    if (Global::config().has("verbose")) {
        os << "SignalHandler::instance()->enableLogging();\n";
    }

    // initialize counter
    os << "// -- initialize counter --\n";
    os << "std::atomic<RamDomain> ctr(0);\n\n";
    os << "std::atomic<size_t> iter(0);\n\n";

    // set default threads (in embedded mode)
    if (std::stoi(Global::config().get("jobs")) > 0) {
        os << "#if defined(__EMBEDDED_SOUFFLE__) && defined(_OPENMP)\n";
        os << "omp_set_num_threads(" << std::stoi(Global::config().get("jobs")) << ");\n";
        os << "#endif\n\n";
    }

    // add actual program body
    os << "// -- query evaluation --\n";
    if (Global::config().has("profile")) {
        os << "ProfileEventSingleton::instance().startTimer();\n";
        os << R"_(ProfileEventSingleton::instance().makeTimeEvent("@time;starttime");)_" << '\n';
        os << "{\n"
           << R"_(Logger logger("@runtime;", 0);)_" << '\n';
    }

    if (Global::config().has("engine")) {
        std::stringstream ss;
        bool hasAtLeastOneStrata = false;
        visitDepthFirst(*(prog.getMain()), [&](const RamStratum& stratum) {
            hasAtLeastOneStrata = true;
            // go to stratum of index in switch
            auto i = stratum.getIndex();
            ss << "case " << i << ":\ngoto STRATUM_" << i << ";\nbreak;\n";
        });
        if (hasAtLeastOneStrata) {
            os << "switch (stratumIndex) {\n";
            {
                // otherwise use stratum 0 if index is -1
                os << "case (size_t) -1:\ngoto STRATUM_0;\nbreak;\n";
            }
            os << ss.str();
            os << "}\n";
        }
    }

    visitDepthFirst(*(prog.getMain()), [&](const RamStratum& stratum) {
        os << "/* BEGIN STRATUM " << stratum.getIndex() << " */\n";
        if (Global::config().has("engine")) {
            // go to the stratum with the max value for int as a suffix if calling the master stratum
            auto i = stratum.getIndex();
            os << "STRATUM_" << i << ":\n";
        }
        os << "{\n";
        emitCode(os, stratum.getBody());
        os << "}\n";
        if (Global::config().has("engine")) {
            os << "if (stratumIndex != (size_t) -1) goto EXIT;\n";
        }

        os << "/* END STRATUM " << stratum.getIndex() << " */\n";
    });

    if (Global::config().has("engine")) {
        os << "EXIT:{}";
    }

    if (Global::config().has("profile")) {
        os << "}\n";
        os << "dumpFreqs();\n";
        os << "ProfileEventSingleton::instance().stopTimer();\n";
        os << "std::ofstream profile(profiling_fname);\n";
        os << "ProfileEventSingleton::instance().dump(profile);\n";
    }

    // add code printing hint statistics
    os << "\n// -- relation hint statistics --\n";
    os << "if(isHintsProfilingEnabled()) {\n";
    os << "std::cout << \" -- Operation Hint Statistics --\\n\";\n";
    visitDepthFirst(*(prog.getMain()), [&](const RamCreate& create) {
        auto name = getRelationName(create.getRelation());
        os << "std::cout << \"Relation " << name << ":\\n\";\n";
        os << name << "->printHintStatistics(std::cout,\"  \");\n";
        os << "std::cout << \"\\n\";\n";
    });
    os << "}\n";

    os << "SignalHandler::instance()->reset();\n";

    os << "}\n";  // end of runFunction() method

    // add methods to run with and without performing IO (mainly for the interface)
    os << "public:\nvoid run(size_t stratumIndex = (size_t) -1) override { runFunction<false>(\".\", \".\", "
          "stratumIndex); }\n";
    os << "public:\nvoid runAll(std::string inputDirectory = \".\", std::string outputDirectory = \".\", "
          "size_t stratumIndex = (size_t) -1) "
          "override { ";
    if (Global::config().has("live-profile")) {
        os << "std::thread profiler([]() { profile::Tui().runProf(); });\n";
    }
    os << "runFunction<true>(inputDirectory, outputDirectory, stratumIndex);\n";
    if (Global::config().has("live-profile")) {
        os << "if (profiler.joinable()) { profiler.join(); }\n";
    }
    os << "}\n";

    // issue printAll method
    os << "public:\n";
    os << "void printAll(std::string outputDirectory = \".\") override {\n";
    visitDepthFirst(*(prog.getMain()), [&](const RamStatement& node) {
        if (auto store = dynamic_cast<const RamStore*>(&node)) {
            for (IODirectives ioDirectives : store->getIODirectives()) {
                os << "try {";
                os << "std::map<std::string, std::string> directiveMap(" << ioDirectives << ");\n";
                os << R"_(if (!outputDirectory.empty() && directiveMap["IO"] == "file" && )_";
                os << "directiveMap[\"filename\"].front() != '/') {";
                os << R"_(directiveMap["filename"] = outputDirectory + "/" + directiveMap["filename"];)_";
                os << "}\n";
                os << "IODirectives ioDirectives(directiveMap);\n";
                os << "IOSystem::getInstance().getWriter(";
                os << "SymbolMask({" << store->getRelation().getSymbolMask() << "})";
                os << ", symTable, ioDirectives, " << Global::config().has("provenance");
                os << ")->writeAll(*" << getRelationName(store->getRelation()) << ");\n";

                os << "} catch (std::exception& e) {std::cerr << e.what();exit(1);}\n";
            }
        } else if (auto print = dynamic_cast<const RamPrintSize*>(&node)) {
            os << "{ auto lease = getOutputLock().acquire(); \n";
            os << "(void)lease;\n";
            os << "std::cout << R\"(" << print->getMessage() << ")\" <<  ";
            os << getRelationName(print->getRelation()) << "->"
               << "size() << std::endl;\n";
            os << "}";
        }
    });
    os << "}\n";  // end of printAll() method

    // dumpFreqs method
    if (Global::config().has("profile")) {
        os << "private:\n";
        os << "void dumpFreqs() {\n";
        for (auto const& cur : idxMap) {
            os << "\tProfileEventSingleton::instance().makeQuantityEvent(R\"_(" << cur.first << ")_\", freqs["
               << cur.second << "],0);\n";
        }
        os << "}\n";  // end of dumpFreqs() method
    }

    // issue loadAll method
    os << "public:\n";
    os << "void loadAll(std::string inputDirectory = \".\") override {\n";
    visitDepthFirst(*(prog.getMain()), [&](const RamLoad& load) {
        // get some table details
        for (IODirectives ioDirectives : load.getIODirectives()) {
            os << "try {";
            os << "std::map<std::string, std::string> directiveMap(";
            os << ioDirectives << ");\n";
            os << R"_(if (!inputDirectory.empty() && directiveMap["IO"] == "file" && )_";
            os << "directiveMap[\"filename\"].front() != '/') {";
            os << R"_(directiveMap["filename"] = inputDirectory + "/" + directiveMap["filename"];)_";
            os << "}\n";
            os << "IODirectives ioDirectives(directiveMap);\n";
            os << "IOSystem::getInstance().getReader(";
            os << "SymbolMask({" << load.getRelation().getSymbolMask() << "})";
            os << ", symTable, ioDirectives";
            os << ", " << Global::config().has("provenance");
            os << ")->readAll(*" << getRelationName(load.getRelation());
            os << ");\n";
            os << "} catch (std::exception& e) {std::cerr << \"Error loading data: \" << e.what() << "
                  "'\\n';}\n";
        }
    });
    os << "}\n";  // end of loadAll() method

    // issue dump methods
    auto dumpRelation = [&](const std::string& name, const SymbolMask& mask, size_t arity) {
        auto relName = name;

        os << "try {";
        os << "IODirectives ioDirectives;\n";
        os << "ioDirectives.setIOType(\"stdout\");\n";
        os << "ioDirectives.setRelationName(\"" << name << "\");\n";
        os << "IOSystem::getInstance().getWriter(";
        os << "SymbolMask({" << mask << "})";
        os << ", symTable, ioDirectives, " << Global::config().has("provenance");
        os << ")->writeAll(*" << relName << ");\n";
        os << "} catch (std::exception& e) {std::cerr << e.what();exit(1);}\n";
    };

    // dump inputs
    os << "public:\n";
    os << "void dumpInputs(std::ostream& out = std::cout) override {\n";
    visitDepthFirst(*(prog.getMain()), [&](const RamLoad& load) {
        auto& name = getRelationName(load.getRelation());
        auto& mask = load.getRelation().getSymbolMask();
        size_t arity = load.getRelation().getArity();
        dumpRelation(name, mask, arity);
    });
    os << "}\n";  // end of dumpInputs() method

    // dump outputs
    os << "public:\n";
    os << "void dumpOutputs(std::ostream& out = std::cout) override {\n";
    visitDepthFirst(*(prog.getMain()), [&](const RamStore& store) {
        auto& name = getRelationName(store.getRelation());
        auto& mask = store.getRelation().getSymbolMask();
        size_t arity = store.getRelation().getArity();
        dumpRelation(name, mask, arity);
    });
    os << "}\n";  // end of dumpOutputs() method

    os << "public:\n";
    os << "const SymbolTable &getSymbolTable() const override {\n";
    os << "return symTable;\n";
    os << "}\n";  // end of getSymbolTable() method

    // TODO: generate code for subroutines
    if (Global::config().has("provenance")) {
        // generate subroutine adapter
        os << "void executeSubroutine(std::string name, const std::vector<RamDomain>& args, "
              "std::vector<RamDomain>& ret, std::vector<bool>& err) override {\n";

        // subroutine number
        size_t subroutineNum = 0;
        for (auto& sub : prog.getSubroutines()) {
            os << "if (name == \"" << sub.first << "\") {\n"
               << "subproof_" << subroutineNum
               << "(args, ret, err);\n"  // subproof_i to deal with special characters in relation names
               << "}\n";
            subroutineNum++;
        }
        os << "}\n";  // end of executeSubroutine

        // generate method for each subroutine
        subroutineNum = 0;
        for (auto& sub : prog.getSubroutines()) {
            // method header
            os << "void "
               << "subproof_" << subroutineNum << "(const std::vector<RamDomain>& args, "
                                                  "std::vector<RamDomain>& ret, std::vector<bool>& err) {\n";

            // generate code for body
            emitCode(os, *sub.second);

            os << "return;\n";
            os << "}\n";  // end of subroutine
            subroutineNum++;
        }
    }

    os << "};\n";  // end of class declaration

    // hidden hooks
    os << "SouffleProgram *newInstance_" << id << "(){return new " << classname << ";}\n";
    os << "SymbolTable *getST_" << id << "(SouffleProgram *p){return &reinterpret_cast<" << classname
       << "*>(p)->symTable;}\n";

    os << "\n#ifdef __EMBEDDED_SOUFFLE__\n";
    os << "class factory_" << classname << ": public souffle::ProgramFactory {\n";
    os << "SouffleProgram *newInstance() {\n";
    os << "return new " << classname << "();\n";
    os << "};\n";
    os << "public:\n";
    os << "factory_" << classname << "() : ProgramFactory(\"" << id << "\"){}\n";
    os << "};\n";
    os << "static factory_" << classname << " __factory_" << classname << "_instance;\n";
    os << "}\n";
    os << "#else\n";
    os << "}\n";
    os << "int main(int argc, char** argv)\n{\n";
    os << "try{\n";

    // parse arguments
    os << "souffle::CmdOptions opt(";
    os << "R\"(" << Global::config().get("") << ")\",\n";
    os << "R\"(.)\",\n";
    os << "R\"(.)\",\n";
    if (Global::config().has("profile")) {
        os << "true,\n";
        os << "R\"(" << Global::config().get("profile") << ")\",\n";
    } else {
        os << "false,\n";
        os << "R\"()\",\n";
    }
    os << std::stoi(Global::config().get("jobs")) << ",\n";
    os << "-1";
    os << ");\n";

    os << "if (!opt.parse(argc,argv)) return 1;\n";

    os << "#if defined(_OPENMP) \n";
    os << "omp_set_nested(true);\n";
    os << "\n#endif\n";

    os << "souffle::";
    if (Global::config().has("profile")) {
        os << classname + " obj(opt.getProfileName());\n";
    } else {
        os << classname + " obj;\n";
    }

#ifdef USE_MPI
    if (Global::config().get("engine") == "mpi") {
        os << "\n#ifdef USE_MPI\n";
        os << "souffle::mpi::init(argc, argv);";
        os << "int rank = souffle::mpi::commRank();";
        os << "int stratum = (rank == 0) ? " << std::numeric_limits<int>::max() << " : rank - 1;";
        os << "obj.runAll(opt.getInputFileDir(), opt.getOutputFileDir(), stratum);\n";
        os << "souffle::mpi::finalize();";
        os << "\n#endif\n";
    } else
#endif
    {
        os << "obj.runAll(opt.getInputFileDir(), opt.getOutputFileDir(), opt.getStratumIndex());\n";
    }

    if (Global::config().get("provenance") == "1") {
        os << "explain(obj, true, false);\n";
    } else if (Global::config().get("provenance") == "2") {
        os << "explain(obj, true, true);\n";
    }
    os << "return 0;\n";
    os << "} catch(std::exception &e) { souffle::SignalHandler::instance()->error(e.what());}\n";
    os << "}\n";
    os << "\n#endif\n";
}

}  // end of namespace souffle
