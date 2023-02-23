package com.herryuser1.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.herryuser1.gmall.realtime.util.DateFormatUtil;
import com.herryuser1.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// 数据流：[web/app -> Nginx -> 日志服务器（.log）]-> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
// 程  序：[ Mock.sh  (lg.sh) ]->Flume(f1.sh)->Kafka(ZK)->BaseLogApp->Kafka(ZK)->DwdTrafficUniqueVistorDetail->Kafka(ZK)

public class DwdTrafficUniqueVistorDetail {
    public static void main(String[] args) throws Exception {
        // 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2 读取Kafka页面日志主题，创建流
        String topic = "dwd_traffic_page_log";
        String groupId = "unique_visitor_detail";
        // 只要是取数据，就是对于执行环境的变量使用addsource，然后从kafka里使用consumer进行消费
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // 3 过滤掉上一跳页面不为null的数据，并将每行数据转化为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    // 获取"page"字段里的"last_page_id"，即上一跳页面ID的情况
                    String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (lastPageId == null) {
                        // 过滤掉这部分的数据
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(s);
                }
            }
        });

        // 4 按照MID分组
        // flink的分组采用keyby
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));//这一句一直没太看懂

        // 5 使用状态编程实现按照Mid的去重功能*****
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            //做一个状态，存日期
            private ValueState<String> lastVisitState;

            // 然后用一个open方法，写状态
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last-visit", String.class);

                // 由于null和非同一日期都会导致状态的更新，一直保留过往数据会造成数据的不必要冗余
                // 因此在此处要设立ttl来确保状态可以定期删除冗余数据
                // 但若直接设置ttl为24小时，可能导致由于清理状态造成的重复统计日活
                // 因此需要让ttl随着状态的更新而更新

                // 先完成第一个功能：让状态保留一天
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)// 完成第二个功能：让其在更新状态的时候重置为1天
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);

                lastVisitState = getRuntimeContext().getState(stateDescriptor);//getState的括号内需要一个状态描述器valueStateDescriptor
            }

            // 接下来要判断状态是否为null，然后把状态和今天的日期相比，因此接下来要获取状态的数据，并且要获取当前数据里的时间戳
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {

                // 获取状态数据&当前数据中的时间戳并转换为日期
                String lastDate = lastVisitState.value();
                Long ts = jsonObject.getLong("ts");
                String curDate = DateFormatUtil.toDate(ts);

                if (lastDate == null || !lastDate.equals(curDate)) {
                    lastVisitState.update(curDate);
                    return true;
                } else {
                    return false;
                }
            }
        });

        // 6 将数据写到Kafka
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        uvDS.print(">>>>>>>>");
        //uvDS.map(json->json.toJSONString())转换为方法调用就写成了下面的样子
        uvDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(targetTopic));

        // 7 启动任务
        env.execute("DwdTrafficUniqueVistorDetail");
    }
}
