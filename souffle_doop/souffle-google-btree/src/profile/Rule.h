/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2016, The Souffle Developers. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

#pragma once

#include <map>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>

namespace souffle {
namespace profile {

/*
 * Class to hold information about souffle Rule profile information
 */
class Rule {
protected:
    std::string name;
    double runtime = 0;
    long num_tuples = 0;
    std::string identifier;
    std::string locator = "";
    std::map<std::tuple<std::string, std::string>, size_t> atoms{};

private:
    bool recursive = false;
    int version = 0;

public:
    Rule(std::string name, std::string id) : name(std::move(name)), identifier(std::move(id)) {}

    Rule(std::string name, int version, std::string id)
            : name(std::move(name)), identifier(std::move(id)), recursive(true), version(version) {}

    inline std::string getId() {
        return identifier;
    }

    inline double getRuntime() {
        return runtime;
    }

    inline long getNum_tuples() {
        return num_tuples;
    }

    inline void setRuntime(double runtime) {
        this->runtime = runtime;
    }

    inline void setNum_tuples(long num_tuples) {
        this->num_tuples = num_tuples;
    }

    inline void addAtomFrequency(const std::string& subruleName, std::string atom, long frequency) {
        atoms[std::make_tuple(subruleName, atom)] = frequency;
    }

    const std::map<std::tuple<std::string, std::string>, size_t>& getAtoms() {
        return atoms;
    }
    inline std::string getName() {
        return name;
    }

    inline void setId(std::string id) {
        identifier = id;
    }

    inline std::string getLocator() {
        return locator;
    }

    void setLocator(std::string locator) {
        this->locator = locator;
    }

    inline bool isRecursive() {
        return recursive;
    }

    inline void setRecursive(bool recursive) {
        this->recursive = recursive;
    }

    inline int getVersion() {
        return version;
    }

    inline void setVersion(int version) {
        this->version = version;
    }

    std::string toString() {
        std::ostringstream output;
        if (recursive) {
            output << "{" << name << "," << version << ":";
        } else {
            output << "{" << name << ":";
        }
        output << "[" << runtime << "," << num_tuples << "]}";
        return output.str();
    }
};

}  // namespace profile
}  // namespace souffle
