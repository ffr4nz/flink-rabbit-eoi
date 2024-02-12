package org.eoi.trash;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.json.JSONObject;

public class Main {

    static RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
            .setHost("DOMAIN")
            .setPort(5672)
            .setUserName("USERNAME")
            .setPassword("PASS")
            .setVirtualHost("VIRTUAL")
            .build();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<String> stream = env
                .addSource(new RMQSource<String>(
                        connectionConfig,            // config for the RabbitMQ connection
                        "eoitestin",                 // name of the RabbitMQ queue to consume
                        true,                        // use correlation ids; can be false if only at-least-once is required
                        new SimpleStringSchema()))   // deserialization schema to turn messages into Java objects
                .setParallelism(1);              // non-parallel source is only required for exactly-once
        stream.print();
        DataStream<JSONObject> json_stream = stream.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                // if we are pulling plain domains
                JSONObject obj = new JSONObject();
                obj.put("domain",s);
                // if we are pulling string with JSON schema
                //JSONObject obj = new JSONObject(s);
                // return obj
                return obj;
            }
        });
        json_stream.print();
        env.execute("Rabbit");
    }
}