
# install thread building blocks (TBB)

include(ExternalProject)

set(TBB_PREFIX ${CMAKE_BINARY_DIR}/tbb-2017_U7)

# add TBB library
ExternalProject_Add(ThreadBuildingBlocks
    URL https://dps.uibk.ac.at/~csaf7445/shared/tbb2017_20170604oss_lin.tgz
#    URL https://github.com/01org/tbb/releases/download/2017_U7/tbb2017_20170604oss_lin.tgz
	PREFIX ${TBB_PREFIX}

    # skip config, build, and install (it is a binary distribution) 
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
)

set(TBB_INCLUDE_DIR ${TBB_PREFIX}/src/ThreadBuildingBlocks/include)

# NOTE: here I fix it to x64 and at least gcc 4.7
set(TBB_LIB_DIR ${TBB_PREFIX}/src/ThreadBuildingBlocks/lib/intel64/gcc4.7)


