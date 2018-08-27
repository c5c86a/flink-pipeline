#!/bin/bash -ex

if [ "$1" == "test" ]; then
  cd bind_mount
  mkdir -p m2
  mvn -Dmaven.repo.local=/opt/flink/bind_mount/m2 clean test
else
  bash "$FLINK_HOME"/bin/start-cluster.sh 
  #bash "$FLINK_HOME"/bin/pyflink-stream.sh "$FLINK_HOME"/bind_mount/example.py
  cd bind_mount
  mvn -Dmaven.repo.local=/opt/flink/bind_mount/m2 javadoc:javadoc javadoc:test-javadoc package
  cd -
  bash "$FLINK_HOME"/bin/flink run -c mygroupid.StreamingJob bind_mount/target/myartifactid-1.0-SNAPSHOT.jar --port 9000
  sleep 10
  # tail while putting the filename as a prefix
  tail -F "$FLINK_HOME"/log/* | awk '/^==> / {a=substr($0, 5, length-8); next} {print a":"$0}'
fi


