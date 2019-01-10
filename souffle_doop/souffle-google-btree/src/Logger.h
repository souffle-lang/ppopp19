/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2013, 2015, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file Logger.h
 *
 * A logger is the utility utilized by RAM programs to create logs and
 * traces.
 *
 ***********************************************************************/

#pragma once

#include "ParallelUtils.h"
#include "ProfileEvent.h"

#include <chrono>
#include <iostream>
#include <utility>

namespace souffle {
/**
 * Obtains a reference to the lock synchronizing output operations.
 */
inline Lock& getOutputLock() {
    static Lock outputLock;
    return outputLock;
}

/**
 * The class utilized to times for the souffle profiling tool. This class
 * is utilized by both -- the interpreted and compiled version -- to conduct
 * the corresponding measurements.
 *
 * To far, only execution times are logged. More events, e.g. the number of
 * processed tuples may be added in the future.
 */
class Logger {
private:
    std::string label;
    time_point start;
    size_t startMaxRSS;
    size_t iteration;

public:
    Logger(std::string label, size_t iteration)
            : label(std::move(label)), start(now()), iteration(iteration) {
        struct rusage ru;
        getrusage(RUSAGE_SELF, &ru);
        startMaxRSS = ru.ru_maxrss;
    }
    ~Logger() {
        struct rusage ru;
        getrusage(RUSAGE_SELF, &ru);
        size_t endMaxRSS = ru.ru_maxrss;
        ProfileEventSingleton::instance().makeTimingEvent(
                label, start, now(), startMaxRSS, endMaxRSS, iteration);
    }
};
}  // end of namespace souffle
