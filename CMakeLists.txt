cmake_minimum_required (VERSION 2.8)

project (xixibase)

include_directories(
    src
    /opt/local/include
    3rd/tinyxml
    3rd/zlib)
link_directories(/opt/local/lib)  
add_executable(xixibase
    src/cache.cpp 
    src/currtime.cpp 
    src/io_service_pool.cpp 
    src/log.cpp 
    src/main.cpp 
    src/server.cpp 
    src/settings.cpp 
    src/stats.cpp 
    src/lookup3.cpp 
    src/util.cpp 
    src/peer.cpp 
    src/peer_cache.cpp 
    src/peer_http.cpp 
    src/peer_pdu.cpp 
    src/auth.cpp
    3rd/zlib/adler32.c
    3rd/zlib/crc32.c
    3rd/zlib/deflate.c
    3rd/zlib/trees.c
    3rd/zlib/zutil.c
    3rd/tinyxml/tinystr.cpp
    3rd/tinyxml/tinyxml.cpp
    3rd/tinyxml/tinyxmlerror.cpp
    3rd/tinyxml/tinyxmlparser.cpp)
target_link_libraries(xixibase
    boost_date_time-mt
    boost_thread-mt
    boost_filesystem-mt
    boost_system-mt
    ssl
    crypto)
SET_TARGET_PROPERTIES(xixibase PROPERTIES #表示生成的执行文件所在路径
RUNTIME_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/bin")
