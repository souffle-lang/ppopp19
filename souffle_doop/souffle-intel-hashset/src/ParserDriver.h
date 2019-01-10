/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file ParserDriver.h
 *
 * Defines the parser driver.
 *
 ***********************************************************************/

#pragma once

#include "SrcLocation.h"
#include "parser.hh"
#include <cstdio>
#include <memory>
#include <string>

namespace souffle {

class AstClause;
class AstComponent;
class AstComponentInit;
class AstIODirective;
class AstPragma;
class AstRelation;
class AstTranslationUnit;
class AstType;
class DebugReport;
class ErrorReport;
class SymbolTable;

using yyscan_t = void*;

struct scanner_data {
    SrcLocation yylloc;

    /* Stack of parsed files */
    const char* yyfilename = nullptr;
};

class ParserDriver {
public:
    ParserDriver();
    virtual ~ParserDriver();

    std::unique_ptr<AstTranslationUnit> translationUnit;

    void addRelation(std::unique_ptr<AstRelation> r);
    void addIODirective(std::unique_ptr<AstIODirective> d);
    void addType(std::unique_ptr<AstType> type);
    void addClause(std::unique_ptr<AstClause> c);
    void addComponent(std::unique_ptr<AstComponent> c);
    void addInstantiation(std::unique_ptr<AstComponentInit> ci);
    void addPragma(std::unique_ptr<AstPragma> p);

    souffle::SymbolTable& getSymbolTable();

    bool trace_scanning = false;

    std::unique_ptr<AstTranslationUnit> parse(const std::string& filename, FILE* in, SymbolTable& symbolTable,
            ErrorReport& errorReport, DebugReport& debugReport);
    std::unique_ptr<AstTranslationUnit> parse(const std::string& code, SymbolTable& symbolTable,
            ErrorReport& errorReport, DebugReport& debugReport);
    static std::unique_ptr<AstTranslationUnit> parseTranslationUnit(const std::string& filename, FILE* in,
            SymbolTable& symbolTable, ErrorReport& errorReport, DebugReport& debugReport);
    static std::unique_ptr<AstTranslationUnit> parseTranslationUnit(const std::string& code,
            SymbolTable& symbolTable, ErrorReport& errorReport, DebugReport& debugReport);

    bool trace_parsing = false;

    void error(const SrcLocation& loc, const std::string& msg);
    void error(const std::string& msg);
};

}  // end of namespace souffle

#define YY_DECL yy::parser::symbol_type yylex(souffle::ParserDriver& driver, yyscan_t yyscanner)
YY_DECL;
