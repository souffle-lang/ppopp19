/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2016, The Souffle Developers. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

#pragma once

#include "../ProfileDatabase.h"
#include "../ProfileEvent.h"
#include "Iteration.h"
#include "ProgramRun.h"
#include "Relation.h"
#include "Rule.h"
#include "StringUtils.h"
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <dirent.h>
#include <sys/stat.h>

namespace souffle {
namespace profile {

namespace {
template <typename T>
class DSNVisitor : public Visitor {
public:
    DSNVisitor(T& base) : base(base) {}
    void visit(TextEntry& text) override {
        if (text.getKey() == "source-locator") {
            base.setLocator(text.getText());
        }
    }
    void visit(DurationEntry& duration) override {
        if (duration.getKey() == "runtime") {
            auto runtime = (duration.getEnd() - duration.getStart()).count() / 1000000.0;
            base.setRuntime(runtime);
        }
    }
    void visit(SizeEntry& size) override {
        if (size.getKey() == "num-tuples") {
            base.setNum_tuples(size.getSize());
        }
    }
    void visit(DirectoryEntry& ruleEntry) override {}

protected:
    T& base;
};

/**
 * Visit ProfileDB atom frequencies.
 * atomrule : {atom: {num-tuples: num}}
 */
class AtomFrequenciesVisitor : public Visitor {
public:
    AtomFrequenciesVisitor(Rule& rule) : rule(rule) {}
    void visit(DirectoryEntry& directory) {
        const std::string& clause = directory.getKey();

        for (auto& key : directory.getKeys()) {
            auto* numTuples =
                    dynamic_cast<SizeEntry*>(directory.readDirectoryEntry(key)->readEntry("num-tuples"));
            if (numTuples == nullptr) {
                return;
            }
            rule.addAtomFrequency(clause, key, numTuples->getSize());
        }
    }

private:
    Rule& rule;
};

/**
 * Visit ProfileDB recursive rule.
 * ruleversion: {DSN}
 */
class RecursiveRuleVisitor : public DSNVisitor<Rule> {
public:
    RecursiveRuleVisitor(Rule& rule) : DSNVisitor(rule) {}
    void visit(DirectoryEntry& directory) override {
        if (directory.getKey() == "atom-frequency") {
            AtomFrequenciesVisitor atomFrequenciesVisitor(base);
            for (auto& key : directory.getKeys()) {
                directory.readDirectoryEntry(key)->accept(atomFrequenciesVisitor);
            }
        }
    }
};

/**
 * Visit ProfileDB non-recursive rules.
 * rule: {versionNum : {DSN}, versionNum+1: {DSN}}
 */
class RecursiveRulesVisitor : public Visitor {
public:
    RecursiveRulesVisitor(Iteration& iteration, Relation& relation)
            : iteration(iteration), relation(relation) {}
    void visit(DirectoryEntry& ruleEntry) override {
        for (const auto& key : ruleEntry.getKeys()) {
            auto& versions = *ruleEntry.readDirectoryEntry(key);
            auto rule = std::make_shared<Rule>(
                    ruleEntry.getKey(), std::stoi(key), relation.createRecID(ruleEntry.getKey()));
            RecursiveRuleVisitor visitor(*rule);
            for (const auto& versionKey : versions.getKeys()) {
                versions.readEntry(versionKey)->accept(visitor);
            }
            // To match map keys defined in Iteration::addRule()
            std::string ruleKey = key + rule->getLocator() + key;
            iteration.addRule(ruleKey, rule);
        }
    }

protected:
    Iteration& iteration;
    Relation& relation;
};

/**
 * Visit ProfileDB non-recursive rule.
 * rule: {DSN}
 */
class NonRecursiveRuleVisitor : public DSNVisitor<Rule> {
public:
    NonRecursiveRuleVisitor(Rule& rule) : DSNVisitor(rule) {}
    void visit(DirectoryEntry& directory) override {
        if (directory.getKey() == "atom-frequency") {
            AtomFrequenciesVisitor atomFrequenciesVisitor(base);
            for (auto& key : directory.getKeys()) {
                directory.readDirectoryEntry(key)->accept(atomFrequenciesVisitor);
            }
        }
    }
};

/**
 * Visit ProfileDB non-recursive rules.
 * non-recursive-rule: {rule1: {DSN}, ...}
 */
class NonRecursiveRulesVisitor : public Visitor {
public:
    NonRecursiveRulesVisitor(Relation& relation) : relation(relation) {}
    void visit(DirectoryEntry& ruleEntry) override {
        auto rule = std::make_shared<Rule>(ruleEntry.getKey(), relation.createID());
        NonRecursiveRuleVisitor visitor(*rule);
        for (const auto& key : ruleEntry.getKeys()) {
            ruleEntry.readEntry(key)->accept(visitor);
        }
        relation.getRuleMap()[rule->getLocator()] = rule;
    }

protected:
    Relation& relation;
};

/**
 * Visit a ProfileDB relation iteration.
 * iterationNumber: {DSN, recursive-rule: {}}
 */
class IterationVisitor : public DSNVisitor<Iteration> {
public:
    IterationVisitor(Iteration& iteration, Relation& relation) : DSNVisitor(iteration), relation(relation) {}
    void visit(DurationEntry& duration) override {
        if (duration.getKey() == "runtime") {
            auto runtime = (duration.getEnd() - duration.getStart()).count() / 1000000.0;
            base.setRuntime(runtime);
        } else if (duration.getKey() == "copytime") {
            auto copytime = (duration.getEnd() - duration.getStart()).count() / 1000000.0;
            base.setCopy_time(copytime);
        }
    }
    void visit(DirectoryEntry& directory) override {
        if (directory.getKey() == "recursive-rule") {
            RecursiveRulesVisitor rulesVisitor(base, relation);
            for (const auto& key : directory.getKeys()) {
                directory.readEntry(key)->accept(rulesVisitor);
            }
        }
        if (directory.getKey() == "maxRSS") {
            auto* preMaxRSS = dynamic_cast<SizeEntry*>(directory.readEntry("pre"));
            auto* postMaxRSS = dynamic_cast<SizeEntry*>(directory.readEntry("post"));
            relation.setPreMaxRSS(preMaxRSS->getSize());
            relation.setPostMaxRSS(postMaxRSS->getSize());
        }
    }

protected:
    Relation& relation;
};

/**
 * Visit ProfileDB iterations.
 * iteration: {num: {}, num2: {}, ...}
 */
class IterationsVisitor : public Visitor {
public:
    IterationsVisitor(Relation& relation) : relation(relation) {}
    void visit(DirectoryEntry& ruleEntry) override {
        auto iteration = std::make_shared<Iteration>();
        relation.getIterations().push_back(iteration);
        IterationVisitor visitor(*iteration, relation);
        for (const auto& key : ruleEntry.getKeys()) {
            ruleEntry.readEntry(key)->accept(visitor);
        }
    }

protected:
    Relation& relation;
};

/**
 * Visit ProfileDB relations.
 * relname: {DSN, non-recursive-rule: {}, iteration: {...}}
 */
class RelationVisitor : public DSNVisitor<Relation> {
public:
    RelationVisitor(Relation& relation) : DSNVisitor(relation) {}
    void visit(DurationEntry& duration) override {
        if (duration.getKey() == "runtime") {
            auto runtime = (duration.getEnd() - duration.getStart()).count() / 1000000.0;
            base.setRuntime(runtime);
        } else if (duration.getKey() == "loadtime") {
            auto loadtime = (duration.getEnd() - duration.getStart()).count() / 1000000.0;
            base.setLoadtime(loadtime);
        } else if (duration.getKey() == "savetime") {
            auto savetime = (duration.getEnd() - duration.getStart()).count() / 1000000.0;
            base.setSavetime(savetime);
        }
    }
    void visit(DirectoryEntry& directory) override {
        if (directory.getKey() == "iteration") {
            IterationsVisitor iterationsVisitor(base);
            for (const auto& key : directory.getKeys()) {
                directory.readEntry(key)->accept(iterationsVisitor);
            }
        } else if (directory.getKey() == "non-recursive-rule") {
            NonRecursiveRulesVisitor rulesVisitor(base);
            for (const auto& key : directory.getKeys()) {
                directory.readEntry(key)->accept(rulesVisitor);
            }
        } else if (directory.getKey() == "maxRSS") {
            auto* preMaxRSS = dynamic_cast<SizeEntry*>(directory.readEntry("pre"));
            auto* postMaxRSS = dynamic_cast<SizeEntry*>(directory.readEntry("post"));
            base.setPreMaxRSS(preMaxRSS->getSize());
            base.setPostMaxRSS(postMaxRSS->getSize());
        }
    }
};
}  // namespace

/*
 * Input reader and processor for log files
 */
class Reader {
private:
    std::string file_loc;
    std::streampos gpos;
    const ProfileDatabase& db = ProfileEventSingleton::instance().getDB();
    bool loaded = false;
    bool online{true};

    double runtime{0};
    std::unordered_map<std::string, std::shared_ptr<Relation>> relation_map{};
    int rel_id{0};

public:
    std::shared_ptr<ProgramRun> run;

    Reader(std::string filename, std::shared_ptr<ProgramRun> run) : file_loc(std::move(filename)), run(run) {
        ProfileEventSingleton::instance().setDBFromFile(file_loc);
    }

    Reader(std::shared_ptr<ProgramRun> run) : run(run) {}
    /**
     * Read the contents from file into the class
     */
    void processFile() {
        rel_id = 0;
        relation_map.clear();
        auto programDuration = dynamic_cast<DurationEntry*>(db.lookupEntry({"program", "runtime"}));
        if (programDuration == nullptr) {
            auto startTimeEntry = dynamic_cast<TimeEntry*>(db.lookupEntry({"program", "starttime"}));
            if (startTimeEntry != nullptr) {
                auto time = startTimeEntry->getTime();
                runtime = (now().time_since_epoch().count() - time.count()) / 1000000.0;
            }
        } else {
            runtime = (programDuration->getEnd() - programDuration->getStart()).count() / 1000000.0;
            online = false;
        }

        auto relations = dynamic_cast<DirectoryEntry*>(db.lookupEntry({"program", "relation"}));
        if (relations == nullptr) {
            // Souffle hasn't generated any profiling information yet.
            return;
        }
        for (const auto& cur : relations->getKeys()) {
            auto relation = dynamic_cast<DirectoryEntry*>(db.lookupEntry({"program", "relation", cur}));
            if (relation != nullptr) {
                addRelation(*relation);
            }
        }
        run->SetRuntime(this->runtime);
        run->setRelation_map(this->relation_map);
        loaded = true;
    }

    void save(std::string f_name);

    void process(const std::vector<std::string>& data) {
        {
            if (data[0].find("frequency") != std::string::npos) {
                if (relation_map.find(data[1]) == relation_map.end()) {
                    relation_map.emplace(data[1], std::make_shared<Relation>(Relation(data[1], createId())));
                }
                std::shared_ptr<Relation> _rel = relation_map[data[1]];
                addFrequency(_rel, data);
            }
        }

        run->SetRuntime(this->runtime);
        run->setRelation_map(this->relation_map);
    }

    inline bool isLive() {
        return online;
    }

    void addRelation(const DirectoryEntry& relation) {
        const std::string& name = relation.getKey();

        relation_map.emplace(name, std::make_shared<Relation>(name, createId()));
        auto& rel = *relation_map[name];
        RelationVisitor relationVisitor(rel);

        for (const auto& key : relation.getKeys()) {
            relation.readEntry(key)->accept(relationVisitor);
        }
    }

    void addIteration(std::shared_ptr<Relation> rel, std::vector<std::string> data);

    void addFrequency(std::shared_ptr<Relation> rel, std::vector<std::string> data) {
        std::unordered_map<std::string, std::shared_ptr<Rule>>& ruleMap = rel->getRuleMap();

        // If we can't find the rule then it must be an Iteration
        if (ruleMap.find(data[3]) == ruleMap.end()) {
            for (auto& iter : rel->getIterations()) {
                for (auto& rule : iter->getRul_rec()) {
                    if (rule.second->getVersion() == std::stoi(data[2])) {
                        rule.second->addAtomFrequency(data[4], data[5], std::stoi(data[7]));
                        return;
                    }
                }
            }
        } else {
            std::shared_ptr<Rule> _rul = ruleMap[data[3]];
            // 0: @frequency-rule;
            // 1: relationName;
            // 2: version;
            // 3: srcloc;
            // 4: stringify(clause);
            // 5: stringify(atom);
            // 6: level;
            // 7: count
            // Generate name from atom + version
            _rul->addAtomFrequency(data[4], data[5], std::stoi(data[7]));
        }
    }

    void addRule(std::shared_ptr<Relation> rel, std::vector<std::string> data);

    inline bool isLoaded() {
        return loaded;
    }

    std::string RelationcreateId() {
        return "R" + std::to_string(++rel_id);
    }

    std::string createId() {
        return "R" + std::to_string(++rel_id);
    }
};

}  // namespace profile
}  // namespace souffle
