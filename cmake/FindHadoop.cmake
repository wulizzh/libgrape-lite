# 仅用于查找并设置 Hadoop (libhdfs.so) 和 JNI (libjvm.so) 库路径的 CMake 模块
#
# 依赖环境变量:
#  HADOOP_HOME: Hadoop 安装目录
#  JAVA_HOME:   Java 安装目录
#
# 设置的变量:
#  HADOOPJNI_FOUND        - 当两个库都找到时为 TRUE
#  HADOOPJNI_LIBRARIES    - 两个库的完整路径列表

include(FindPackageHandleStandardArgs)

if(NOT UNIX OR APPLE)
    message(STATUS "Hadoop/JNI support is disabled on non-Linux systems")
    set(HADOOPJNI_FOUND FALSE)
    return()
endif()

#=====================================
# Hadoop (libhdfs.so) 检测
#=====================================
set(HADOOP_LIBRARY_FOUND FALSE)

if(DEFINED ENV{HADOOP_HOME})
    set(HADOOP_LIB_PATH "$ENV{HADOOP_HOME}/lib/native/libhdfs.so")
    
    if(EXISTS ${HADOOP_LIB_PATH})
        set(HADOOP_LIBRARY_FOUND TRUE)
        set(HADOOP_LIBRARY ${HADOOP_LIB_PATH})
        message(STATUS "Found libhdfs.so: ${HADOOP_LIBRARY}")
    else()
        message(STATUS "Could NOT find libhdfs.so at ${HADOOP_LIB_PATH}")
    endif()
else()
    message(STATUS "HADOOP_HOME environment variable not found")
endif()

#=====================================
# JNI (libjvm.so) 检测
#=====================================
set(JNI_LIBRARY_FOUND FALSE)

if(DEFINED ENV{JAVA_HOME})
    set(JNI_LIB_PATH "$ENV{JAVA_HOME}/jre/lib/amd64/server/libjvm.so")
    
    if(EXISTS ${JNI_LIB_PATH})
        set(JNI_LIBRARY_FOUND TRUE)
        set(JNI_LIBRARY ${JNI_LIB_PATH})
        message(STATUS "Found libjvm.so: ${JNI_LIBRARY}")
    else()
        message(STATUS "Could NOT find libjvm.so at ${JNI_LIB_PATH}")
    endif()
else()
    message(STATUS "JAVA_HOME environment variable not found")
endif()

#=====================================
# 组合结果
#=====================================
set(HADOOPJNI_FOUND ${HADOOP_LIBRARY_FOUND} AND ${JNI_LIBRARY_FOUND})

if(HADOOPJNI_FOUND)
    set(HADOOPJNI_LIBRARIES ${HADOOP_LIBRARY} ${JNI_LIBRARY})
    mark_as_advanced(HADOOP_LIBRARY JNI_LIBRARY)
endif()