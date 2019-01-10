/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2016, The Souffle Developers. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

#pragma once

#include <string>

namespace souffle {
namespace profile {

class CellInterface {
public:
    virtual std::string toString(int precision) = 0;

    virtual double getDoubVal() = 0;

    virtual long getLongVal() = 0;

    virtual std::string getStringVal() = 0;

    virtual ~CellInterface() {}
};

}  // namespace profile
}  // namespace souffle
