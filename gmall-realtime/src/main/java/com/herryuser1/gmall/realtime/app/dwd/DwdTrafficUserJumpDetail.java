package com.herryuser1.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.herryuser1.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        // 1 获取执行环境
        //和其他两个需求是一样的
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2 读取Kafka页面日志，创建流
        // 和独立访客的需求情况是一样的
        String topic = "dwd_traffic_page_log";
        String groupId = "user_jump_detail";
        // 只要是取数据，就是对于执行环境的变量使用addsource，然后从kafka里使用consumer进行消费
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // 3 将数据转化为JSON对象
        // 两种方法都不是很懂
        //kafkaDS.map(line -> JSON.parseObject(line));
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        // 4 提取事件时间 按照MID分组
        // 为什么要先提取时间戳再分组？
        //
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        }))
                .keyBy(json -> json.getJSONObject("common").getString("mid"));

        // 5 定义CEP的模式序列
//        Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject jsonObject) throws Exception {
//                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
//            }
//        }).next("next").where(new SimpleCondition<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject jsonObject) throws Exception {
//                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
//            }
//        }).within(Time.seconds(10));

        //Times默认是宽松近邻，所以这边还需要加上严格近邻.consecutive()的限制，否则只是相当于followBy
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return jsonObject.getJSONObject("page").getString("last_page_id") == null;
                    }
                }).times(2)
                .consecutive()
                .within(Time.seconds(10));

        // 6 将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // 7 提取事件（匹配上的事件+超时事件）
        OutputTag<String> timeOutTag = new OutputTag<String>("timeOut"){
        };
        SingleOutputStreamOperator<String> selectDS = patternStream.select(timeOutTag,
                new PatternTimeoutFunction<JSONObject, String>() {
                    @Override
                    public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("start").get(0).toJSONString();
                    }
                }, new PatternSelectFunction<JSONObject, String>() {
                    @Override
                    public String select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0).toJSONString();
                    }
                });
        DataStream<String> timeOutDS = selectDS.getSideOutput(timeOutTag);

        // 8 合并两种事件
        DataStream<String> unionDS = selectDS.union(timeOutDS);

        // 9 将数据写到Kafka
        selectDS.print("Select>>>>>>>>>>");
        timeOutDS.print("TimeOut>>>>>>>>");
        String targetTopic = "dwd_traffic_user_jump_detail";
        unionDS.addSink(MyKafkaUtil.getKafkaProducer(targetTopic));

        // 10 启动任务
        env.execute("DwdTrafficUserJumpDetail");
    }
}
