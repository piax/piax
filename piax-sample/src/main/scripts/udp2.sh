#!/bin/sh
#
# Copyright (c) 2016 PIAX development team
#
cd "$(dirname "$0")"

# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ]; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done

# Get standard environment variables
PRGDIR=`dirname "$PRG"`

# Only set PIAX_HOME if not already set
[ -f "$PIAX_HOME"/bin/udp2.sh ] || PIAX_HOME=`cd "$PRGDIR/.." ; pwd`
export PIAX_HOME

# Set JavaHome if it exists
if [ -f "${JAVA_HOME}/bin/java" ]; then 
   JAVA=${JAVA_HOME}/bin/java
else
   JAVA=java
fi
export JAVA

LOG_FILE=$PIAX_HOME/config/logging.properties
JAVA_OPTS_SCRIPT="-XX:+HeapDumpOnOutOfMemoryError -Djava.awt.headless=true"

$JAVA $JAVA_OPTS $JAVA_OPTS_SCRIPT -Djava.util.logging.config.file="$LOG_FILE" -cp "$PIAX_HOME/lib/*" org.piax.samples.gtrans.hello.udp.sender.Main localhost 10001 localhost 10000
