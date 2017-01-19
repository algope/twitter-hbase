#!/bin/sh
# ----------------------------------------------------------------------------
#  Copyright 2001-2006 The Apache Software Foundation.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# ----------------------------------------------------------------------------
#
#   Copyright (c) 2001-2006 The Apache Software Foundation.  All rights
#   reserved.


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

PRGDIR=`dirname "$PRG"`
BASEDIR=`cd "$PRGDIR/.." >/dev/null; pwd`



# OS specific support.  $var _must_ be set to either true or false.
cygwin=false;
darwin=false;
case "`uname`" in
  CYGWIN*) cygwin=true ;;
  Darwin*) darwin=true
           if [ -z "$JAVA_VERSION" ] ; then
             JAVA_VERSION="CurrentJDK"
           else
             echo "Using Java version: $JAVA_VERSION"
           fi
		   if [ -z "$JAVA_HOME" ]; then
		      if [ -x "/usr/libexec/java_home" ]; then
			      JAVA_HOME=`/usr/libexec/java_home`
			  else
			      JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Versions/${JAVA_VERSION}/Home
			  fi
           fi       
           ;;
esac

if [ -z "$JAVA_HOME" ] ; then
  if [ -r /etc/gentoo-release ] ; then
    JAVA_HOME=`java-config --jre-home`
  fi
fi

# For Cygwin, ensure paths are in UNIX format before anything is touched
if $cygwin ; then
  [ -n "$JAVA_HOME" ] && JAVA_HOME=`cygpath --unix "$JAVA_HOME"`
  [ -n "$CLASSPATH" ] && CLASSPATH=`cygpath --path --unix "$CLASSPATH"`
fi

# If a specific java binary isn't specified search for the standard 'java' binary
if [ -z "$JAVACMD" ] ; then
  if [ -n "$JAVA_HOME"  ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
      # IBM's JDK on AIX uses strange locations for the executables
      JAVACMD="$JAVA_HOME/jre/sh/java"
    else
      JAVACMD="$JAVA_HOME/bin/java"
    fi
  else
    JAVACMD=`which java`
  fi
fi

if [ ! -x "$JAVACMD" ] ; then
  echo "Error: JAVA_HOME is not defined correctly." 1>&2
  echo "  We cannot execute $JAVACMD" 1>&2
  exit 1
fi

if [ -z "$REPO" ]
then
  REPO="$BASEDIR"/repo
fi

CLASSPATH="$BASEDIR"/etc:"$REPO"/org/apache/hbase/hbase-client/0.98.6-cdh5.3.5/hbase-client-0.98.6-cdh5.3.5.jar:"$REPO"/org/apache/hbase/hbase-common/0.98.6-cdh5.3.5/hbase-common-0.98.6-cdh5.3.5.jar:"$REPO"/commons-collections/commons-collections/3.2.1/commons-collections-3.2.1.jar:"$REPO"/org/apache/hadoop/hadoop-core/2.5.0-mr1-cdh5.3.5/hadoop-core-2.5.0-mr1-cdh5.3.5.jar:"$REPO"/tomcat/jasper-runtime/5.5.23/jasper-runtime-5.5.23.jar:"$REPO"/tomcat/jasper-compiler/5.5.23/jasper-compiler-5.5.23.jar:"$REPO"/javax/servlet/jsp/jsp-api/2.1/jsp-api-2.1.jar:"$REPO"/javax/servlet/servlet-api/2.5/servlet-api-2.5.jar:"$REPO"/hsqldb/hsqldb/1.8.0.10/hsqldb-1.8.0.10.jar:"$REPO"/org/eclipse/jdt/core/3.1.1/core-3.1.1.jar:"$REPO"/log4j/log4j/1.2.17/log4j-1.2.17.jar:"$REPO"/org/apache/hbase/hbase-protocol/0.98.6-cdh5.3.5/hbase-protocol-0.98.6-cdh5.3.5.jar:"$REPO"/commons-codec/commons-codec/1.7/commons-codec-1.7.jar:"$REPO"/commons-io/commons-io/2.4/commons-io-2.4.jar:"$REPO"/commons-lang/commons-lang/2.6/commons-lang-2.6.jar:"$REPO"/commons-logging/commons-logging/1.1.1/commons-logging-1.1.1.jar:"$REPO"/com/google/protobuf/protobuf-java/2.5.0/protobuf-java-2.5.0.jar:"$REPO"/io/netty/netty/3.6.6.Final/netty-3.6.6.Final.jar:"$REPO"/org/apache/zookeeper/zookeeper/3.4.5-cdh5.3.5/zookeeper-3.4.5-cdh5.3.5.jar:"$REPO"/org/slf4j/slf4j-api/1.7.5/slf4j-api-1.7.5.jar:"$REPO"/org/slf4j/slf4j-log4j12/1.7.5/slf4j-log4j12-1.7.5.jar:"$REPO"/org/cloudera/htrace/htrace-core/2.04/htrace-core-2.04.jar:"$REPO"/org/mortbay/jetty/jetty-util/6.1.26/jetty-util-6.1.26.jar:"$REPO"/org/codehaus/jackson/jackson-mapper-asl/1.8.8/jackson-mapper-asl-1.8.8.jar:"$REPO"/org/codehaus/jackson/jackson-core-asl/1.8.8/jackson-core-asl-1.8.8.jar:"$REPO"/org/apache/hadoop/hadoop-common/2.5.0-cdh5.3.5/hadoop-common-2.5.0-cdh5.3.5.jar:"$REPO"/commons-cli/commons-cli/1.2/commons-cli-1.2.jar:"$REPO"/org/apache/commons/commons-math3/3.1.1/commons-math3-3.1.1.jar:"$REPO"/xmlenc/xmlenc/0.52/xmlenc-0.52.jar:"$REPO"/commons-httpclient/commons-httpclient/3.1/commons-httpclient-3.1.jar:"$REPO"/commons-net/commons-net/3.1/commons-net-3.1.jar:"$REPO"/org/mortbay/jetty/jetty/6.1.26.cloudera.4/jetty-6.1.26.cloudera.4.jar:"$REPO"/com/sun/jersey/jersey-core/1.9/jersey-core-1.9.jar:"$REPO"/com/sun/jersey/jersey-json/1.9/jersey-json-1.9.jar:"$REPO"/org/codehaus/jettison/jettison/1.1/jettison-1.1.jar:"$REPO"/com/sun/xml/bind/jaxb-impl/2.2.3-1/jaxb-impl-2.2.3-1.jar:"$REPO"/javax/xml/bind/jaxb-api/2.2.2/jaxb-api-2.2.2.jar:"$REPO"/javax/xml/stream/stax-api/1.0-2/stax-api-1.0-2.jar:"$REPO"/javax/activation/activation/1.1/activation-1.1.jar:"$REPO"/org/codehaus/jackson/jackson-jaxrs/1.8.3/jackson-jaxrs-1.8.3.jar:"$REPO"/org/codehaus/jackson/jackson-xc/1.8.3/jackson-xc-1.8.3.jar:"$REPO"/commons-el/commons-el/1.0/commons-el-1.0.jar:"$REPO"/net/java/dev/jets3t/jets3t/0.9.0/jets3t-0.9.0.jar:"$REPO"/org/apache/httpcomponents/httpcore/4.1.2/httpcore-4.1.2.jar:"$REPO"/com/jamesmurty/utils/java-xmlbuilder/0.4/java-xmlbuilder-0.4.jar:"$REPO"/commons-configuration/commons-configuration/1.6/commons-configuration-1.6.jar:"$REPO"/commons-digester/commons-digester/1.8/commons-digester-1.8.jar:"$REPO"/commons-beanutils/commons-beanutils/1.7.0/commons-beanutils-1.7.0.jar:"$REPO"/commons-beanutils/commons-beanutils-core/1.8.0/commons-beanutils-core-1.8.0.jar:"$REPO"/org/apache/avro/avro/1.7.6-cdh5.3.5/avro-1.7.6-cdh5.3.5.jar:"$REPO"/com/thoughtworks/paranamer/paranamer/2.3/paranamer-2.3.jar:"$REPO"/org/xerial/snappy/snappy-java/1.0.5/snappy-java-1.0.5.jar:"$REPO"/com/google/code/gson/gson/2.2.4/gson-2.2.4.jar:"$REPO"/com/jcraft/jsch/0.1.42/jsch-0.1.42.jar:"$REPO"/org/apache/curator/curator-client/2.6.0/curator-client-2.6.0.jar:"$REPO"/org/apache/curator/curator-recipes/2.6.0/curator-recipes-2.6.0.jar:"$REPO"/com/google/code/findbugs/jsr305/1.3.9/jsr305-1.3.9.jar:"$REPO"/org/apache/commons/commons-compress/1.4.1/commons-compress-1.4.1.jar:"$REPO"/org/tukaani/xz/1.0/xz-1.0.jar:"$REPO"/org/apache/hadoop/hadoop-auth/2.5.0-cdh5.3.5/hadoop-auth-2.5.0-cdh5.3.5.jar:"$REPO"/org/apache/httpcomponents/httpclient/4.2.5/httpclient-4.2.5.jar:"$REPO"/org/apache/directory/server/apacheds-kerberos-codec/2.0.0-M15/apacheds-kerberos-codec-2.0.0-M15.jar:"$REPO"/org/apache/directory/server/apacheds-i18n/2.0.0-M15/apacheds-i18n-2.0.0-M15.jar:"$REPO"/org/apache/directory/api/api-asn1-api/1.0.0-M20/api-asn1-api-1.0.0-M20.jar:"$REPO"/org/apache/directory/api/api-util/1.0.0-M20/api-util-1.0.0-M20.jar:"$REPO"/org/apache/curator/curator-framework/2.6.0/curator-framework-2.6.0.jar:"$REPO"/org/apache/hadoop/hadoop-annotations/2.5.0-cdh5.3.5/hadoop-annotations-2.5.0-cdh5.3.5.jar:"$REPO"/com/github/stephenc/findbugs/findbugs-annotations/1.3.9-1/findbugs-annotations-1.3.9-1.jar:"$REPO"/junit/junit/4.11/junit-4.11.jar:"$REPO"/org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar:"$REPO"/com/googlecode/json-simple/json-simple/1.1.1/json-simple-1.1.1.jar:"$REPO"/com/google/guava/guava/20.0/guava-20.0.jar:"$REPO"/org/upm/hbase/twitterhbase/twitterhbase/1.0-SNAPSHOT/twitterhbase-1.0-SNAPSHOT.jar

ENDORSED_DIR=
if [ -n "$ENDORSED_DIR" ] ; then
  CLASSPATH=$BASEDIR/$ENDORSED_DIR/*:$CLASSPATH
fi

if [ -n "$CLASSPATH_PREFIX" ] ; then
  CLASSPATH=$CLASSPATH_PREFIX:$CLASSPATH
fi

# For Cygwin, switch paths to Windows format before running java
if $cygwin; then
  [ -n "$CLASSPATH" ] && CLASSPATH=`cygpath --path --windows "$CLASSPATH"`
  [ -n "$JAVA_HOME" ] && JAVA_HOME=`cygpath --path --windows "$JAVA_HOME"`
  [ -n "$HOME" ] && HOME=`cygpath --path --windows "$HOME"`
  [ -n "$BASEDIR" ] && BASEDIR=`cygpath --path --windows "$BASEDIR"`
  [ -n "$REPO" ] && REPO=`cygpath --path --windows "$REPO"`
fi

exec "$JAVACMD" $JAVA_OPTS -server \
  -classpath "$CLASSPATH" \
  -Dapp.name="hbaseApp.sh" \
  -Dapp.pid="$$" \
  -Dapp.repo="$REPO" \
  -Dapp.home="$BASEDIR" \
  -Dbasedir="$BASEDIR" \
  org.upm.hbase.twitterhbase.App \
  "$@"
