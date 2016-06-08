#!/bin/bash
########################################################################################################################
# 
#  Copyright (C) 2013 by the DIMA Research Group, TU Berlin (http://www.dima.tu-berlin.de)
# 
#  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
#  the License. You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
#  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
#  specific language governing permissions and limitations under the License.
# 
########################################################################################################################

# These are used to mangle paths that are passed to java when using 
# cygwin. Cygwin paths are like linux paths, i.e. /path/to/somewhere
# but the windows java version expects them in Windows Format, i.e. C:\bla\blub.
# "cygpath" can do the conversion.
manglePath() {
    UNAME=$(uname -s)
    if [ "${UNAME:0:6}" == "CYGWIN" ]; then
        echo `cygpath -w $1`
    else
        echo $1
    fi
}

manglePathList() {
    UNAME=$(uname -s)
    # a path list, for example a java classpath
    if [ "${UNAME:0:6}" == "CYGWIN" ]; then
        echo `cygpath -wp $1`
    else
        echo $1
    fi
}

########################################################################################################################
# DEFAULT CONFIG VALUES
########################################################################################################################

DEFAULT_JAVA_HOME="/usr/lib/jvm/java-8-oracle"      # Java home
DEFAULT_JAVA_OPTS=""                                # Optional JVM parameters

########################################################################################################################
# PATHS AND CONFIG
########################################################################################################################

# Resolve links
this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# Convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

# Define the main directory of the Nephele installation
PEEL_ROOT_DIR=`dirname "$this"`
PEEL_LIB_DIR=${PEEL_ROOT_DIR}/lib

########################################################################################################################
# ENVIRONMENT VARIABLES
########################################################################################################################

# Define JAVA_HOME if it is not already set
if [ -z "${JAVA_HOME}" ]; then
    JAVA_HOME=${DEFAULT_JAVA_HOME}
fi

if [ -z "${PEEL_JAVA_OPTS}" ]; then
    PEEL_JAVA_OPTS="${DEFAULT_JAVA_OPTS}"
fi

UNAME=$(uname -s)
if [ "${UNAME:0:6}" == "CYGWIN" ]; then
    JAVA_RUN=java
else
    if [[ -d ${JAVA_HOME} ]]; then
        JAVA_RUN=${JAVA_HOME}/bin/java
    else
        JAVA_RUN=java
    fi
fi

# Default classpath 
CLASSPATH=`manglePathList $( echo ${PEEL_LIB_DIR}/*.jar . | sed 's/ /:/g' )`

# Run command
${JAVA_RUN} -cp ${CLASSPATH} ${PEEL_JAVA_OPTS} org.peelframework.core.cli.Peel $@
