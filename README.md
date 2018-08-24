[![Build Status](https://travis-ci.org/nicosmaris/flink-piepeline.svg?branch=master)](https://travis-ci.org/nicosmaris/flink-pipeline)

TODO: read from input file

To send data to elasticsearch `.addSink(new ESSink())` and:
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