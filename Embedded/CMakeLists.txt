add_library(DBEngine SHARED DBETypes.cpp DBEngine.cpp DBEngine.h DBETypes.h)

target_link_libraries(DBEngine QueryRunner QueryEngine DataMgr ${Arrow_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_DL_LIBS} ${TBB_LIBS})

add_executable(EmbeddedDbTest EmbeddedDbTest.cpp)
target_link_libraries(EmbeddedDbTest DBEngine)

find_program(PYTHON "python3")
IF(PYTHON)
    set(SETUP_PY    "${CMAKE_CURRENT_BINARY_DIR}/setup.py")
    # Currently dbe.cpp is generated near dbe.pyx and not in build dir. This could be revised later
    set(OUTPUT_DBE  "${CMAKE_CURRENT_SOURCE_DIR}/dbe.cpp")

    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/setup.in.py" "${CMAKE_CURRENT_BINARY_DIR}/setup.py.in" @ONLY)
    # cannot be done in one step, splitting in configure_file and file commands
    file(GENERATE OUTPUT "${SETUP_PY}" INPUT "${CMAKE_CURRENT_BINARY_DIR}/setup.py.in")

    add_custom_command(OUTPUT ${OUTPUT_DBE}
        DEPENDS DBEngine ${SETUP_PY} ${CMAKE_CURRENT_SOURCE_DIR}/dbe.pyx ${CMAKE_CURRENT_SOURCE_DIR}/DBEngine.pxd
        COMMAND cd ${CMAKE_CURRENT_BINARY_DIR} && python3 ${SETUP_PY} build_ext -g -f -I ${CMAKE_CURRENT_SOURCE_DIR}
        )

    add_custom_target(Python ALL DEPENDS ${OUTPUT_DBE})

    add_custom_target(editable-install
        cd ${CMAKE_CURRENT_BINARY_DIR} && pip install -e .
        DEPENDS Python)

    install(CODE "execute_process(COMMAND python3 ${SETUP_PY} install -f)")

ENDIF(PYTHON)
#TODO: Some assertion if no Python was found

