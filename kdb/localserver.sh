#!/bin/bash

nohup java -server -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -cp target/kdb-1.0-SNAPSHOT.jar kdb.Transport conf/datanode1.properties &
nohup java -server -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -cp target/kdb-1.0-SNAPSHOT.jar kdb.Transport conf/datanode2.properties &
nohup java -server -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -cp target/kdb-1.0-SNAPSHOT.jar kdb.Transport conf/datanode3.properties &
