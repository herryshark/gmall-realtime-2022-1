package com.herryuser1.gmall.realtime.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class MyKafkaUtil {

    private static final String KAFKA_SERVER = "hadoop102:9092";

    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic, String groupId){

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);

        return new FlinkKafkaConsumer<String>(
                topic,
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                        if (consumerRecord==null || consumerRecord.value()==null){
                            return "";
                        }else {
                            return new String(consumerRecord.value());
                        }
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                },
                properties);
    }

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {

        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        return new FlinkKafkaProducer<String>(topic,
                new KafkaSerializationSchema<String>(){

                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
                        if (s == null){
                            return new ProducerRecord<>(topic,"".getBytes());
                        }
                        return new ProducerRecord<>(topic,s.getBytes());
                    }
                },prop,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    /**
     * Kafka-Source DDL 语句
     *
     * @param topic   数据源主题
     * @param groupId 消费者组
     * @return 拼接好的 Kafka 数据源 DDL 语句
     */
    public static String getKafkaDDL(String topic, String groupId) {
        return " with ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'group-offsets')";
    }

    /**
     * topic_db主题的 Kafka-Source DDL 语句
     * @param groupId 消费者组
     * @return        拼接好的Kafka数据源DDL语句
     */
    // +I[gmall, cart_info, insert, {is_ordered=1, cart_price=123456.0, create_time=2023-02-22 01:14:14, sku_num=22, sku_id=1, source_type=1, order_time=2023-02-23 01:14:51, user_id=A, img_url=11112, is_checked=1, sku_name=DFD, id=1, source_id=1, operate_time=2023-02-23 00:00:00}, null, 2023-02-22T17:14:54.567Z]
    // +I[gmall, cart_info, update, {is_ordered=1, cart_price=123456.0, create_time=2023-02-22 01:14:14, sku_num=22, sku_id=2, source_type=1, order_time=2023-02-23 01:14:51, user_id=A, img_url=11112, is_checked=1, sku_name=DFD, id=1, source_id=1, operate_time=2023-02-23 00:00:00}, {sku_id=1}, 2023-02-22T17:16:29.990Z]
    public static String getTopicDb(String groupId){
        return "CREATE TABLE topic_db ( " +
                "  `database` STRING, " +
                "  `table` STRING, " +
                "  `type` STRING, " +
                "  `data` MAP<STRING,STRING>, " +
                "  `old` MAP<STRING,STRING>, " +
                "  `pt` AS PROCTIME() " +
                ") "+getKafkaDDL("topic_db",groupId);
    }
}
