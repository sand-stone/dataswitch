
1. Install the native dll 
   cp libs/libwiredtiger-2.8.1.dylib /usr/local/lib/
   mvn install:install-file -Dfile=wiredtiger.jar -DgroupId=wiredtiger -DartifactId=wiredtiger -Dversion=2.8.1 -Dpackaging=jar

2. Execute Java
   java -cp target/XXX.jar -Djava.library.path=./libs/darwin aJar
