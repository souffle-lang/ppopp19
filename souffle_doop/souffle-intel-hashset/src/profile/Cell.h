/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2016, The Souffle Developers. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

#pragma once

#include "CellInterface.h"
#include "StringUtils.h"

#include <iostream>
#include <string>
#include <utility>

namespace souffle {
namespace profile {

template <typename T>
class Cell : public CellInterface {
public:
    T val;
    Cell(T value) : val(value){};
    virtual ~Cell() {}
};

template <>
class Cell<double> : public CellInterface {
public:
    double val;
    Cell(double value) : val(value){};
    double getDoubVal() override {
        return val;
    }
    long getLongVal() override {
        std::cerr << "getting long on double cell\n";
        throw this;
    }
    std::string getStringVal() override {
        std::cerr << "getting string on double cell\n";
        throw this;
    }
    std::string toString(int precision) override {
        return Tools::formatTime(val);
    }
};

template <>
class Cell<std::string> : public CellInterface {
public:
    std::string val;
    Cell(std::string value) : val(std::move(value)){};
    double getDoubVal() override {
        std::cerr << "getting double on string cell\n";
        throw this;
    }
    long getLongVal() override {
        std::cerr << "getting long on string cell\n";
        throw this;
    }
    std::string getStringVal() override {
        return val;
    }
    std::string toString(int precision) override {
        return Tools::cleanString(val);
    }
};

template <>
class Cell<long> : public CellInterface {
public:
    long val;
    Cell(long value) : val(value){};
    double getDoubVal() override {
        std::cerr << "getting double on long cell\n";
        throw this;
    }
    std::string getStringVal() override {
        std::cerr << "getting string on long cell\n";
        throw this;
    }
    long getLongVal() override {
        return val;
    }
    std::string toString(int precision) override {
        return Tools::formatNum(precision, val);
    };
};

template <>
class Cell<void> : public CellInterface, std::false_type {
public:
    Cell() = default;
    double getDoubVal() override {
        std::cerr << "getting double on void cell";
        throw this;
    }
    long getLongVal() override {
        std::cerr << "getting long on void cell";
        throw this;
    }
    std::string getStringVal() override {
        std::cerr << "getting string on void cell\n";
        throw this;
    }
    std::string toString(int precision) override {
        return "-";
    }
};

}  // namespace profile
}  // namespace souffle
