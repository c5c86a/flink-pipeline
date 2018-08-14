#!/bin/bash -ex


bash "$FLINK_HOME"/bin/start-cluster.sh 

#bash "$FLINK_HOME"/bin/pyflink-stream.sh "$FLINK_HOME"/bind_mount/example.py

cd bind_mount
mvn package
cd -

bash "$FLINK_HOME"/bin/flink run -c mygroupid.StreamingJob bind_mount/target/original-myartifactid-1.0-SNAPSHOT.jar --port 9000
sleep 10

#tail -F "$FLINK_HOME"/log/flink-*-taskexecutor-*.out 
grep -H "" "$FLINK_HOME"/log/flink-*-taskexecutor-*.out 
 
