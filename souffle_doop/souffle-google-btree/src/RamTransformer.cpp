/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file RamTransformer.cpp
 *
 * Defines the interface for RAM transformation passes.
 *
 ***********************************************************************/

#include "RamTransformer.h"
#include "RamTranslationUnit.h"

namespace souffle {

bool RamTransformer::apply(RamTranslationUnit& translationUnit) {
    bool changed = transform(translationUnit);
    if (changed) {
        translationUnit.invalidateAnalyses();
    }
    return changed;
}

}  // end of namespace souffle
