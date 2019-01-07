#
# Builds the insieme core library
#
project(Souffle_DS_Eval CXX)

include(cmake/environment.cmake)

# enable ctest support for testing
enable_testing()

# States that CMake required version must be >= 2.6
cmake_minimum_required(VERSION 2.6)

# Create a variable called insieme_compiler_core_SOURCES containing all .cpp files:
file(GLOB_RECURSE project_srcs 	src/*.cpp src/*.def src/*.inc )
file(GLOB_RECURSE project_incs 	include/*.h include/*.def include/*.inc )

# list all include paths
include_directories( ${project_inc_dir} )
include_directories( ${TBB_INCLUDE_DIR} )

# add include directory
link_directories( ${TBB_LIB_DIR} )

# Build the code as a library
add_library(project_lib ${LINKING_TYPE} ${project_srcs} ${project_incs})
add_dependencies( project_lib ThreadBuildingBlocks )


# build executables
file(GLOB_RECURSE executables src/*.cxx)
foreach ( exec_file ${executables})

    get_filename_component( exec_name ${exec_file} NAME_WE )

    # Build dummy executable
    add_executable( ${exec_name} ${exec_file}  )
    add_dependencies( ${exec_name} ThreadBuildingBlocks )

	target_link_libraries(${exec_name} project_lib)
	target_link_libraries(${exec_name} ${pthread_lib})
	target_link_libraries(${exec_name} -lgomp )
	target_link_libraries(${exec_name} -ltbb )
	
endforeach(exec_file)


# Build unit tests
set ( ut_prefix  ut_ )
file(GLOB_RECURSE test_cases test/*.cc)
foreach ( case_file ${test_cases})
	get_filename_component( case_name ${case_file} NAME_WE )
	
	set ( case_name ${ut_prefix}${case_name} )
	add_executable(${case_name} ${case_file})
    add_dependencies( ${case_name} ThreadBuildingBlocks )

	target_link_libraries(${case_name} project_lib)
	target_link_libraries(${case_name} ${pthread_lib})
	target_link_libraries(${case_name} -lgomp )
	target_link_libraries(${case_name} -ltbb )

	add_unit_test(${case_name} ${ut_prefix})
endforeach(case_file)

