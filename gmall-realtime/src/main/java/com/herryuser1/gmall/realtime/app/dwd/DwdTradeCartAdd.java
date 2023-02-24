package com.herryuser1.gmall.realtime.app.dwd;

import com.herryuser1.gmall.realtime.util.MyKafkaUtil;
import com.herryuser1.gmall.realtime.util.MysqlUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        // 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);//这一句是新增加的

//        // 1.1 开启CheckPoint
//        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10*60000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
//
//        // 1.2 设置状态后端.
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig()setCheckpointStorage("hdfs://hadoop102:8020/230130/ck");
//        System.setProperty("HADOOP_USER_NAME","herryuser1");

        // 2 使用DDL方式读取topic_db主题的数据创建表
        tableEnv.executeSql(MyKafkaUtil.getTopicDb("cart_add"));
//        Table query = tableEnv.sqlQuery("select * from topic_db");
//        tableEnv.toAppendStream(query, Row.class)
//                .print(">>>>>>");

        // 3 过滤出加购数据
        Table cartAddTable = tableEnv.sqlQuery("" +
                "select " +
                "    `data`['id'] id, " +
                "    `data`['user_id'] user_id, " +
                "    `data`['sku_id'] sku_id, " +
                "    `data`['cart_price'] cart_price, " +
                "    if(`type`='insert',`data`['sku_num'],cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)) sku_num, " +
                "    `data`['sku_name'] sku_name, " +
                "    `data`['is_checked'] is_checked, " +
                "    `data`['create_time'] create_time, " +
                "    `data`['operate_time'] operate_time, " +
                "    `data`['is_ordered'] is_ordered, " +
                "    `data`['order_time'] order_time, " +
                "    `data`['source_type'] source_type, " +
                "    `data`['source_id'] source_id, " +
                "    pt " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'cart_info' " +
                "and `type` = 'insert' " +//探寻为什么此处要加括号
                "or (`type` = 'update'  " +
                "    and  " +
                "    `old`['sku_num'] is not null  " +
                "    and  " +
                "    cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int))");

        //将架构表转化为流并进行打印
        tableEnv.toAppendStream(cartAddTable, Row.class)
                .print(">>>>>>");

        // 4 读取MySQL的base_dic表作为Lookup表 维表退化
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // 5 关联两张表
        tableEnv.sqlQuery(""
                +"");

        // 6 用FlinkSQL 使用DDL方式，创建加购事实表

        // 7 将数据写出

        // 8 写出任务
        env.execute("DwdTradeCartAdd");
    }
}
