#!/bin/bash -ex

if [ "$1" == "test" ]; then
  cd bind_mount
  mkdir -p m2
  mvn -Dmaven.repo.local=/opt/flink/bind_mount/m2 test
else
  bash "$FLINK_HOME"/bin/start-cluster.sh 
  #bash "$FLINK_HOME"/bin/pyflink-stream.sh "$FLINK_HOME"/bind_mount/example.py
  cd bind_mount
  mvn -Dmaven.repo.local=/opt/flink/bind_mount/m2 package
  cd -
  bash "$FLINK_HOME"/bin/flink run -c mygroupid.StreamingJob bind_mount/target/original-myartifactid-1.0-SNAPSHOT.jar --port 9000
  sleep 10
  grep -H "" "$FLINK_HOME"/log/* 
fi


