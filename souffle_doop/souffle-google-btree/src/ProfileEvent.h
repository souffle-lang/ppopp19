/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2018, The Souffle Developers. All rights reserved.
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file ProfileEvent.h
 *
 * Declares classes for profile events
 *
 ***********************************************************************/

#pragma once

#include "EventProcessor.h"
#include "ProfileDatabase.h"
#include "Util.h"
#include <atomic>
#include <cassert>
#include <chrono>
#include <ctime>
#include <functional>
#include <iomanip>
#include <iostream>
#include <list>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <sys/resource.h>
#include <sys/time.h>

namespace souffle {

/**
 * Profile Event Singleton
 */
class ProfileEventSingleton {
    /** profile database */
    profile::ProfileDatabase database;

    ProfileEventSingleton() = default;

public:
    ~ProfileEventSingleton() {
        stopTimer();
    }

    /** get instance */
    static ProfileEventSingleton& instance() {
        static ProfileEventSingleton singleton;
        return singleton;
    }

    /** create time event */
    void makeTimeEvent(const std::string& txt) {
        profile::EventProcessorSingleton::instance().process(
                database, txt.c_str(), std::chrono::duration_cast<microseconds>(now().time_since_epoch()));
    }

    /** create an event for recording start and end times */
    void makeTimingEvent(const std::string& txt, time_point start, time_point end, size_t startMaxRSS,
            size_t endMaxRSS, size_t iteration) {
        microseconds start_ms = std::chrono::duration_cast<microseconds>(start.time_since_epoch());
        microseconds end_ms = std::chrono::duration_cast<microseconds>(end.time_since_epoch());
        profile::EventProcessorSingleton::instance().process(
                database, txt.c_str(), start_ms, end_ms, startMaxRSS, endMaxRSS, iteration);
    }

    /** create quantity event */
    void makeQuantityEvent(const std::string& txt, size_t number, int iteration) {
        profile::EventProcessorSingleton::instance().process(database, txt.c_str(), number, iteration);
    }

    /** create utilisation event */
    void makeUtilisationEvent(const std::string& txt) {
        /* current time */
        microseconds time = std::chrono::duration_cast<microseconds>(now().time_since_epoch());
        /* system CPU time used */
        struct rusage ru;
        getrusage(RUSAGE_SELF, &ru);
        /* system CPU time used */
        uint64_t systemTime = ru.ru_stime.tv_sec * 1000000 + ru.ru_stime.tv_usec;
        /* user CPU time used */
        uint64_t userTime = ru.ru_utime.tv_sec * 1000000 + ru.ru_utime.tv_usec;
        /* Maximum resident set size (kb) */
        size_t maxRSS = ru.ru_maxrss;

        profile::EventProcessorSingleton::instance().process(
                database, txt.c_str(), time, systemTime, userTime, maxRSS);
    }

    /** Dump all events */
    void dump(std::ostream& os) {
        database.print(os);
    }

    /** Start timer */
    void startTimer() {
        timer.start();
    }

    /** Stop timer */
    void stopTimer() {
        timer.stop();
    }

    const profile::ProfileDatabase& getDB() const {
        return database;
    }

    void setDBFromFile(const std::string& filename) {
        database = profile::ProfileDatabase(filename);
    }

private:
    /**  Profile Timer */
    class ProfileTimer {
    private:
        /** time interval between per utilisation read */
        uint32_t t;

        /** timer is running */
        bool running = false;

        /** thread timer runs on */
        std::thread th;

        /** number of utilisation events */
        uint32_t runCount = 0;

        /** run method for thread th */
        void run() {
            ProfileEventSingleton::instance().makeUtilisationEvent("@utilisation");
            ++runCount;
            if (runCount % 128 == 0) {
                increaseInterval();
            }
        }

        uint32_t getInterval() {
            return t;
        }

        /** Increase value of time interval by factor of 2 */
        void increaseInterval() {
            // Don't increase time interval past 60 seconds
            if (t < 60000) {
                t = t * 2;
            }
        }

    public:
        ProfileTimer(uint32_t in = 1) : t(in) {}

        /** start timer on the thread th */
        void start() {
            running = true;

            th = std::thread([this]() {
                while (running) {
                    run();
                    std::this_thread::sleep_for(std::chrono::milliseconds(getInterval()));
                }
            });
        }

        /** stop timer on the thread th */
        void stop() {
            running = false;
            if (th.joinable()) {
                th.join();
            }
        }
    };

    ProfileTimer timer;
};

}  // namespace souffle
