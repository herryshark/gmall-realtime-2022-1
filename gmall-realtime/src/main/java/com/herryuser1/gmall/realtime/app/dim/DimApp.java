package com.herryuser1.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.herryuser1.gmall.realtime.app.func.DimSinkFunction;
import com.herryuser1.gmall.realtime.app.func.TableProcessFunction;
import com.herryuser1.gmall.realtime.bean.TableProcess;
import com.herryuser1.gmall.realtime.util.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DimApp {
    public static void main(String[] args) throws Exception {

        // 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

            // 1.1 开启CheckPoint
            env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setCheckpointTimeout(10*60000L);
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));

            // 1.2 设置状态后端
            env.setStateBackend(new HashMapStateBackend());
            env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/230130/ck");
            System.setProperty("HADOOP_USER_NAME","herryuser1");

        // 2 读取Kafka topic_db 主题数据创建主流
        String topic = "topic_db";
        String groupId = "dimapp_230130";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // 3 过滤掉非JSON数据 保留新增、变化及初始化数据并将数据转化为JSON格式
        SingleOutputStreamOperator<JSONObject> filterJsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    // 将数据转换为JSON格式
                    JSONObject jsonObject = JSON.parseObject(s);

                    // 获取数据中的操作类型字段
                    String type = jsonObject.getString("type");

                    // 保留新增、变化以及初始化数据
                    if ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type)) {
                        collector.collect(jsonObject);
                    }


                } catch (Exception e) {
                    System.out.println("发现脏数据：" + s);
                }

            }
        });

        // 4 使用FlinkCDC读取MySQL配置信息表创建配置流
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("215225")
                .databaseList("gmall-config")
                .tableList("gmall-config.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> mysqlSourceDS = env.fromSource(mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MysqlSource");

        // 5 将配置流处理为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlSourceDS.broadcast(mapStateDescriptor);

        // 6 连接主流与广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterJsonObjDS.connect(broadcastStream);

        // 7 处理连接流，根据配置信息处理主流数据
        SingleOutputStreamOperator<JSONObject> dimDS = connectedStream.process(new TableProcessFunction(mapStateDescriptor));

        // 8 将数据写出到Phoenix
        dimDS.print(">>>>>>>>>>");
        dimDS.addSink(new DimSinkFunction());

        // 9 启动任务
        env.execute("DimApp");

    }
}