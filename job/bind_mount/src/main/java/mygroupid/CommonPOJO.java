package mygroupid;

import java.util.HashMap;

/**
 * A POJO with public fields, an empty constructor and toString for Flink.
 * Each field is initialized to avoid NPE.
 */
public class CommonPOJO {
    /**
     * This is a demo of a custom deserialization schema with one field that has a default value.
     * If we addSource FlinkKafkaConsumer with ConfluentRegistryAvroDeserializationSchema
     * and SpecificRecord then we can update the schema at the Schema Registry and
     * all job instances will use the new default values when processing.
     * A doc field and a version field at the schema of type record can help
     * us in troubleshooting.
     * For an example of Avro, check:
     * https://medium.com/@stephane.maarek/introduction-to-schemas-in-apache-kafka-with-the-confluent-schema-registry-3bf55e401321
     * "The consumer does cache the schemas, and they don’t change often, so you get a cost at first read, but then the rest is cost-free"
     */
    public HashMap<String, String> schema;

    public String cachedSchemaID = "";
    public Integer deliveryDelay = -1;
    public Boolean isLessThanThreshold = false;
    public Boolean isLessThanMovingAvg = false;

    public CommonPOJO() {
    }
    public CommonPOJO(String cachedSchemaID, Integer deliveryDelay){
        if(cachedSchemaID.equals("1"))
            this.schema = new HashMap<String, String>(){{
                put("deliveryDelayThreshold", "10");
            }};
        else if(cachedSchemaID.equals("2"))
            this.schema = new HashMap<String, String>(){{
                put("deliveryDelayThreshold", "100");
            }};
        else if(cachedSchemaID.equals("3"))
            this.schema = new HashMap<String, String>(){{
                put("invalidAlert", "5");
            }};
        else
            this.schema = new HashMap<String, String>();
        this.deliveryDelay = deliveryDelay;
        this.cachedSchemaID = cachedSchemaID;
    }
    @Override
    public String toString() {
        return "(" +
                cachedSchemaID + "," +
                deliveryDelay + "," +
                (isLessThanThreshold?"true":"false") + "," +
                (isLessThanMovingAvg?"true":"false") +
                ")";
    }
}

