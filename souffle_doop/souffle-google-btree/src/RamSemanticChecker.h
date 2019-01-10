/*
 * Souffle - A Datalog Compiler
 *  Copyright (c) 2018, The Souffle Developers. All rights reserved
 *  Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file RamSemanticChecker.h
 *
 * Defines semantic checker pass for RAM
 *
 ***********************************************************************/

#pragma once

#include "RamTransformer.h"
#include "RamTranslationUnit.h"

namespace souffle {

/**
 * Semantic checker for RAM programs
 */
// TODO (#541): not implemented yet
class RamSemanticChecker : public RamTransformer {
private:
    bool transform(RamTranslationUnit& translationUnit) override {
        // TODO (#541): implementation missing
        return true;
    }

public:
    std::string getName() const override {
        return "SemanticChecker";
    }
};

}  // end of namespace souffle
