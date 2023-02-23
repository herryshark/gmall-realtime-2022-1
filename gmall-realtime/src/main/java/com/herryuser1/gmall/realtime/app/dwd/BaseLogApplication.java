package com.herryuser1.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.herryuser1.gmall.realtime.util.DateFormatUtil;
import com.herryuser1.gmall.realtime.util.MyKafkaUtil;
import org.apache.commons.math3.fitting.leastsquares.LeastSquaresProblem;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

// 数据流：web/app -> Nginx -> 日志服务器（.log） -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
// 程  序：[     Mock.sh   (lg.sh)     ] -> Flume(f1.sh) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK)

public class BaseLogApplication {
    public static void main(String[] args) throws Exception {
        // 1 获取执行环境 直接从DimApp复制的
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.1 开启CheckPoint
//        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10*60000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));

        // 1.2 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/230131/ck1");
        System.setProperty("HADOOP_USER_NAME","herryuser1");

        // 2 消费kafka topic_log 主题的数据 创建流
        String topic = "topic_log";
        String groupId = "base_log_app";
        //开始消费
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // 3 过滤掉非JSON格式的数据，将每行数据转换为JSON对象
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty"){
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {

                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyTag, s);
                }
            }
        });
        // 获取侧输出流脏数据并打印
        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.print("Dirty>>>>>>>>>>");

        // 4 新老访客标签校验 应该用键控状态，每个MID应该有自己的状态，所以要根据mid进行分组
        // 先获取common字段，然后从common字段中获取mid
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        // 5 使用状态编程  这是个啥 对新老访客进行标记校验
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            // 定义状态
            private ValueState<String> lastVisitState;

            // 用open方法，如果是skella的话可以用rude
            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-visit", String.class));
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                // 先获取common字段，然后获取is_new标记& ts
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                Long ts = jsonObject.getLong("ts");

                // 将时间戳转化为年月日
                String curDate = DateFormatUtil.toDate(ts);

                // 获取状态中的日期
                String lastDate = lastVisitState.value();

                // 判断is_new标记是否为1
                if ("1".equals(isNew)) {
                    if (lastDate == null) {
                        lastVisitState.update(curDate);
                    } else if (!lastDate.equals(curDate)) {//说是这里因为一定不等于null所以一定不会空指针，空指针会怎么样
                        jsonObject.getJSONObject("common").put("is_new", "0");
                    }
                } else if (lastDate == null) {
                    lastVisitState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L));
                }
                return jsonObject;
            }
        });

        // 6 使用侧输出流进行分流处理 要分5个流，所以要有4个侧输出流的标记 本次尝试将页面日志放到主流 启动 曝光 动作 错误放到侧输出流
        // 启动 页面（曝光+动作） 错误，启动和页面是互斥关系
        // 因此编译侧输出流的时候，可以先把 错误 摘出，排除掉错误的情况后，先处理启动，再处理页面，最后研究是曝光还是动作
        OutputTag<String> startTag = new OutputTag<String>("start"){
        };
        OutputTag<String> displayTag = new OutputTag<String>("display"){
        };
        OutputTag<String> actionTag = new OutputTag<String>("action"){
        };
        OutputTag<String> errorTag = new OutputTag<String>("error"){
        };
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {

                // 尝试获取错误信息
                String err = jsonObject.getString("err");
                if (err != null) {
                    //将数据写到error侧输出流
                    context.output(errorTag, jsonObject.toJSONString());
                }

                //移除错误信息
                jsonObject.remove("err");

                //尝试获取启动信息
                String start = jsonObject.getString("start");
                if (start != null) {
                    //将数据写到start侧输出流
                    context.output(startTag, jsonObject.toJSONString());
                } else {
                    //获取公共信息&页面id&时间戳
                    String common = jsonObject.getString("common");
                    String pageId = jsonObject.getJSONObject("page").getString("page_id");
                    Long ts = jsonObject.getLong("ts");
                    //尝试获取曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //遍历曝光数据&写到display侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("page_id", pageId);
                            display.put("ts", ts);
                            context.output(displayTag, display.toJSONString());
                        }
                    }

                    //尝试获取动作数据
                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        //遍历曝光数据&写到display侧输出流
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("page_id", pageId);
                            context.output(actionTag, action.toJSONString());

                        }
                    }

                    //移除曝光和动作数据&写到主流
                    jsonObject.remove("displays");
                    jsonObject.remove("actions");
                    collector.collect(jsonObject.toJSONString());

                }

            }
        });

        // 7 提取各个侧输出数据
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);

        // 8 将数据打印并写入对应的主题
        pageDS.print("Page>>>>>>>>>>");
        startDS.print("Start>>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>>");
        actionDS.print("Action>>>>>>>>>>");
        errorDS.print("Error>>>>>>>>>>");

        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDS.addSink(MyKafkaUtil.getKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getKafkaProducer(start_topic));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer(display_topic));
        actionDS.addSink(MyKafkaUtil.getKafkaProducer(action_topic));
        errorDS.addSink(MyKafkaUtil.getKafkaProducer(error_topic));

        // 9 启动任务
        env.execute("BaseLogApplication");
    }
}
