

1. Start metadata server
java -Djava.library.path=./libs/darwin/ -cp target/slipstream-1.0-SNAPSHOT.jar slipstream.MetaDataServer conf/metadata.properties

2. Start Data servers
java -Djava.library.path=./libs/darwin/ -cp ./target/slipstream-1.0-SNAPSHOT.jar slipstream.DataServer conf/dataserver1.properties
java -Djava.library.path=./libs/darwin/ -cp ./target/slipstream-1.0-SNAPSHOT.jar slipstream.DataServer conf/dataserver2.properties
java -Djava.library.path=./libs/darwin/ -cp ./target/slipstream-1.0-SNAPSHOT.jar slipstream.DataServer conf/dataserver3.properties

3. Start Gateway server
java -cp ./target/slipstream-1.0-SNAPSHOT.jar slipstream.GatewayServer conf/gateway.properties

4. Upload a test file
curl -i -F filedata=@./src/test/data/slipstream.000001 http://localhost:9080/mysql
