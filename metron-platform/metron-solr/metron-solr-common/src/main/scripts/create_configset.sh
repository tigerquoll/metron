#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
METRON_VERSION=${project.version}
METRON_HOME=/usr/metron/$METRON_VERSION
ZOOKEEPER=${ZOOKEEPER:-localhost:2181}
ZOOKEEPER_HOME=${ZOOKEEPER_HOME:-/usr/hdp/current/zookeeper-client}
SECURITY_ENABLED=${SECURITY_ENABLED:-false}
NEGOTIATE=''
if [ ${SECURITY_ENABLED,,} == 'true' ]; then
    NEGOTIATE=' --negotiate -u : '
fi

# Get the first Solr node from the list of live nodes in Zookeeper
SOLR_NODE=`$ZOOKEEPER_HOME/bin/zkCli.sh -server $ZOOKEEPER ls /live_nodes | tail -n 1 | sed 's/\[\([^,]*\).*\]/\1/' | sed 's/_solr//'`

# test for errors in SOLR URL
if [[ ${SOLR_NODE} =~ .*:null ]]; then
 echo "Error occurred while attempting to read SOLR Cloud configuration data from Zookeeper.";
 if ! [[ ${ZOOKEEPER} =~ .*/solr ]]; then
   echo "Warning! Environment variable ZOOKEEPER=$ZOOKEEPER does not contain a chrooted zookeeper ensemble address - are you sure you do not mean ZOOKEEPER=$ZOOKEEPER/solr?";
 fi
 exit 1;
fi

# test for presence of datetime field in schema collection
DQT='"'
DATETIME_SCHEMA="<field name=${DQT}datetime${DQT} type=${DQT}datetime${DQT}"
grep --quiet --fixed-strings $DATETIME_SCHEMA $METRON_HOME/config/schema/$1/schema.xml; rc=$?
if [ $rc -eq 1 ]; then
  echo "could not find $DATETIME_SCHEMA in schema file - please read Metron SOLR Time based alias instructions before using this script"
  exit 1;
fi

# Upload the collection config set
zip -rj - $METRON_HOME/config/schema/$1 | curl -X POST $NEGOTIATE --header "Content-Type:text/xml" --data-binary @- "http://$SOLR_NODE/solr/admin/configs?action=UPLOAD&name=$1"

echo "Configset $1 successfully uploaded"
