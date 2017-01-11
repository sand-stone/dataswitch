The basic workflow is to sink the change events from MySQL, and write into the kdb, then read the events out of kdb and write into another MySQL table.

With the exception of kdb, more work is needed on the other pieces in particular regards to config., sharding, error handling etc.

Now at least we have a minimum demoware going. I think I also need to learn about the Kubernetes ecosystem and see how we could do the deployment/config. and use things like nigix reverse proxy.

==use a different terminal for each step below ====
1. After MySQL install, and add the following to /etc/my.cnf, then restart mysql
[mysqld]
server-id=12345
log-bin=slipstream
gtid_mode=on
log-slave-updates
enforce-gtid-consistency
binlog_format=row
binlog_rows_query_log_events
expire_logs_days=1

From mysql shell and create the following two tables (same schema, one is the original table, and the other is replication).

CREATE DATABASE acme;
use acme;

CREATE TABLE Employees(
        id INT NOT NULL AUTO_INCREMENT,
        first VARCHAR(100) NOT NULL,
        last VARCHAR(40) NOT NULL,
        age int,
        PRIMARY KEY ( id )
   );

CREATE TABLE Employees2(
        id INT NOT NULL AUTO_INCREMENT,
        first VARCHAR(100) NOT NULL,
        last VARCHAR(40) NOT NULL,
        age int,
        PRIMARY KEY ( id )
   );


2. cd dataswitch/kdb and start up the kdb server
    mvn clean package
    ./kdb.sh conf/datanode.properties/

3. Start up the MySQL binlog listener (under account root/mysql)
   java -cp target/slipstream-1.0-SNAPSHOT.jar slipstream.MySQLReplicationStream root mysql
   Nov 27, 2016 1:23:26 PM com.github.shyiko.mysql.binlog.BinaryLogClient connect
INFO: Connected to localhost:3306 at 60e0443e-b4d1-11e6-8ac7-24658e9850c8:1-1406 (sid:65535, cid:32)
This program could listen in on the mysql binlog change and streaming the events data out. [The MySQLBinLogReader could read a binlog file offline]

4. cd into dataswitch/slipstream/gen, then compile and run
    javac -cp slipstream-1.0-SNAPSHOT.jar MySQLDataGen.java
    java -cp slipstream-1.0-SNAPSHOT.jar:. MySQLDataGen
This should insert 100 rows into the Employees table. You can verify it via mysql shell. From the #3 terminal, you should see
   13:23:32.272 [main] INFO  slipstream.KdbConnector - send 100 records

5. Now we should have some mysql event data in the kdb. Start the program from terminal to read it out and write into the Employees2 table
   java -cp target/slipstream-1.0-SNAPSHOT.jar slipstream.MySQLWriter
After this program exists, you could verify the data in the Employees2 table
Currently the schema mapping data is encoded as a constant string in MySQLWriter.java . It can be externalized pretty easily.
