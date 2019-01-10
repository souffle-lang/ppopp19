/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2016, The Souffle Developers. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

#pragma once

#include "StringUtils.h"
#include "Tui.h"

#include <iostream>
#include <map>
#include <string>
#include <vector>

namespace souffle {
namespace profile {

/*
 * CLI to parse command line arguments and start up the TUI to either run a single command,
 * generate the GUI file or run the TUI
 */
class Cli {
public:
    std::vector<std::string> args;

    Cli(int argc, char* argv[]) : args() {
        for (int i = 0; i < argc; i++) {
            args.push_back(std::string(argv[i]));
        }
    }

    void error() {
        std::cout << "Unknown error.\nTry souffle-profile -h for help.\n";
        exit(1);
    }

    void parse() {
        if (args.size() == 1) {
            std::cout << "No arguments provided.\nTry souffle-profile -h for help.\n";
            exit(1);
        }

        switch (args.at(1).at(0)) {
            case '-':
                switch (args.at(1).at(args.at(1).length() - 1)) {
                    case 'h':
                        std::cout << "Souffle Profiler" << std::endl
                                  << "Usage: souffle-profile -v | -h | <log-file> [ -c <command> [options] |"
                                     "-j | -l ]"
                                  << std::endl
                                  << "<log-file>            The log file to profile." << std::endl
                                  << "-c <command>          Run the given command on the log file, try with  "
                                     "'-c help' for a list"
                                  << std::endl
                                  << "                      of commands." << std::endl
                                  << "-j                    Generate a GUI (html/js) version of the profiler."
                                  << std::endl
                                  << "-l                    Run the profiler in live mode." << std::endl
                                  << "                      try with '-o help' for more information."
                                  << std::endl
                                  << "-v                    Print the profiler version." << std::endl
                                  << "-h                    Print this help message." << std::endl;
                        break;
                    case 'v':
                        std::cout << "Souffle Profiler v3.0.1\n";
                        break;
                    default:
                        std::cout << "Unknown option " << args.at(1)
                                  << ".\nTry souffle-profile -h for help.\n";
                        exit(1);
                        break;
                }
                break;
            default:

                std::string filename = args.at(1);

                if (args.size() == 2) {
                    Tui(filename, false, false).runProf();

                } else {
                    switch (args.at(2).at(args.at(2).length() - 1)) {
                        case 'c':
                            if (args.size() == 3) {
                                std::cout << "No arguments provided for option " << args.at(2)
                                          << ".\nTry souffle-profile -h for help.\n";
                                exit(1);
                            }
                            Tui(filename, false, false).runCommand(Tools::split(args.at(3), " "));
                            break;
                        case 'l':
                            Tui(filename, true, false).runProf();
                            break;
                        case 'j':
                            Tui(filename, false, true).outputJson();
                            break;
                    }
                }
                break;
        }
    }
};

}  // namespace profile
}  // namespace souffle
