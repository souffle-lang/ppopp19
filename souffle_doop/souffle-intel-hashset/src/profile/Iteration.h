/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2016, The Souffle Developers. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

#pragma once

#include "Rule.h"
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace souffle {
namespace profile {

/*
 * Represents recursive profile data
 */
class Iteration {
private:
    double runtime = 0;
    long num_tuples = 0;
    double copy_time = 0;
    std::string locator = "";
    long prev_num_tuples = 0;

    std::unordered_map<std::string, std::shared_ptr<Rule>> rul_rec_map;

public:
    Iteration() : rul_rec_map() {}

    void addRule(const std::string& ruleKey, std::shared_ptr<Rule>& rule) {
        rul_rec_map[ruleKey] = rule;
    }
    void addRule(std::vector<std::string> data, std::string rec_id) {
        std::string strTemp = data[4] + data[3] + data[2];

        std::shared_ptr<Rule> rul_rec = nullptr;
        if (rul_rec_map.find(strTemp) == rul_rec_map.end()) {
            rul_rec = std::make_shared<Rule>(Rule(data[4], std::stoi(data[2]), rec_id));
            rul_rec->setRuntime(0);
            rul_rec_map[strTemp] = rul_rec;
        } else {
            rul_rec = rul_rec_map[strTemp];
        }

        if (data[0].at(0) == 't') {
            rul_rec->setRuntime(std::stod(data[7]) + rul_rec->getRuntime());
            rul_rec->setLocator(data[3]);
        } else if (data[0].at(0) == 'n') {
            rul_rec->setNum_tuples(std::stol(data[5]) - prev_num_tuples);
            this->prev_num_tuples = std::stol(data[5]);
        }
    }

    inline const std::unordered_map<std::string, std::shared_ptr<Rule>>& getRul_rec() {
        return this->rul_rec_map;
    }

    std::string toString() {
        std::ostringstream output;

        output << runtime << "," << num_tuples << "," << copy_time << ",";
        output << " recRule:";
        for (auto& rul : rul_rec_map) output << rul.second->toString();
        output << "\n";
        return output.str();
    }

    inline double getRuntime() {
        return runtime;
    }

    inline void setRuntime(double runtime) {
        this->runtime = runtime;
    }

    inline long getNum_tuples() {
        return num_tuples;
    }

    inline void setNum_tuples(long num_tuples) {
        this->num_tuples = num_tuples;
    }

    inline double getCopy_time() {
        return copy_time;
    }

    inline void setCopy_time(double copy_time) {
        this->copy_time = copy_time;
    }

    inline std::string getLocator() {
        return locator;
    }

    inline void setLocator(std::string locator) {
        this->locator = locator;
    }
};

}  // namespace profile
}  // namespace souffle
