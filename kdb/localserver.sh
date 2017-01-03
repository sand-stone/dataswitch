#!/bin/bash
nohup java -server -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -cp target/kdb-1.0-SNAPSHOT.jar kdb.Transport conf/datanode1.properties &
nohup java -server -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -cp target/kdb-1.0-SNAPSHOT.jar kdb.Transport conf/datanode2.properties &
nohup java -server -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -cp target/kdb-1.0-SNAPSHOT.jar kdb.Transport conf/datanode3.properties &
