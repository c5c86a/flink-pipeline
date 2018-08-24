[![Build Status](https://travis-ci.org/nicosmaris/flink-piepeline.svg?branch=master)](https://travis-ci.org/nicosmaris/flink-pipeline)

To process data with flink and set it to redis, `docker-compose up job redis redis-ui` and click on the first type that you see at:

http://localhost:5001/myredis:6379/0/keys/

To send data to elasticsearch replace addSink call at StreamingJob with `.addSink(new ESSink())` and:
```
docker-compose up -d elasticsearch
# wait for 30 seconds
curl -XPUT localhost:9200/documents -d '{
    "mappings": {
        "document": {
            "properties": {
                "text": {
                    "type": "string"
                }
            }
        }
    }
}'
docker-compose up job
```

