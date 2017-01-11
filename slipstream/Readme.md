From the kdb terminal, start up the kdb (single node mode)
.  ./kdb.sh conf/datanode.properties
You can tail nohup file to see if the server starts up properly (you could use ./clean script to stop the server and wipe the data directory clean)

From slipstream terminal, run the simple console program
 java -cp target/slipstream-1.0-SNAPSHOT.jar slipstream.MySQLBinLogReader src/test/data/slipstream.000001 http://localhost:8000
