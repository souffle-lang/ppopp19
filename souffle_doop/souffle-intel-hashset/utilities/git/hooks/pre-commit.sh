#!/bin/bash

set -e -u -o pipefail

if [ $(which clang-format-4.0) ]
then
    clang-format-4.0 \
            -i \
            -style=file \
            src/*.h \
            src/*.cpp \
            src/test/*.h \
            src/test/*.cpp \
            tests/interface/*/*.cpp
fi
