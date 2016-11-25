
mvn install:install-file -Dfile=rocksdbjni-4.13.2-osx.jar -DgroupId=rocksdb -DartifactId=rocksdb -Dversion=4.13.2 -Dpackaging=jar

mvn install:install-file -Dfile=rocksdbjni-4.13.2-linux64.jar -DgroupId=rocksdb -DartifactId=rocksdb -Dversion=4.13.2 -Dpackaging=jar

export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64/
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_102.jdk/Contents/Home/

make shared_lib;make rocksdbjava
make rocksdbjavastaticrelease
