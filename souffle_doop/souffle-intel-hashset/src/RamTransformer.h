/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file RamTransformer.h
 *
 * Defines the interface for RAM transformation passes.
 *
 ***********************************************************************/

#pragma once

#include <string>

namespace souffle {

class RamTranslationUnit;

/**
 * Abstract Transformer Class for RAM
 */

class RamTransformer {
private:
    /** Transform RAM translation unit */
    virtual bool transform(RamTranslationUnit& translationUnit) = 0;

public:
    virtual ~RamTransformer() = default;

    /** Apply transformer */
    bool apply(RamTranslationUnit& translationUnit);

    /** Get name of transformer */
    virtual std::string getName() const = 0;
};

}  // end of namespace souffle
