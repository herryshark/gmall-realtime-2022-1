package com.herryuser1.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.herryuser1.gmall.realtime.bean.TableProcess;
import com.herryuser1.gmall.realtime.common.GmallConfig;
import com.sun.xml.internal.fastinfoset.alphabet.BuiltInRestrictedAlphabets;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import static com.sun.xml.internal.fastinfoset.alphabet.BuiltInRestrictedAlphabets.table;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection connection;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //{"before":null,"after":{"source_table":"aa","sink_table":"bb","sink_columns":"cc",
    // "sink_pk":"id","sink_extend":"xxx"},"source":{"version":"1.5.4.Final","connector":"mysql",
    // "name":"mysql_binlog_source","ts_ms":1675270337754,"snapshot":"false",
    // "db":"gmall-config","sequence":null,"table":"table_process","server_id":0,
    // "gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r",
    // "ts_ms":1675270337757,"transaction":null}
    @Override
    public void processBroadcastElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
        // 1 获取并解析数据
        // 把整个变成jsonObject

        JSONObject jsonObject = JSON.parseObject(s);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        // 2 校验并建表
        checkTable(tableProcess.getSinkTable(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkPk(),
                tableProcess.getSinkExtend()
        );

        // 3 写入状态广播出去
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable(),tableProcess);

    }

    /**
     * @param sinkTable   Phoenix表名
     * @param sinkColumns    Phoenix表字段
     * @param sinkPk    Phoenix表主键
     * @param sinkExtend    Phoenix表扩展字段
     */

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;

        try {
            //处理特殊字段
            if (sinkPk == null || "".equals(sinkPk)){
                sinkPk = "id";
            }
            if (sinkExtend == null){
                sinkExtend="";

            }

            //拼接SQL
            StringBuilder createTableSql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {

                //取出字段
                String column = columns[i];

                //判断是否为主键
                if (sinkPk.equals(column)){
                    createTableSql.append(column).append(" varchar primary key");
                }else {
                    createTableSql.append(column).append(" varchar");
                }

                // 判断是否是最后一个字段
                if (i < columns.length - 1){
                    createTableSql.append(",");
                }

            }

            createTableSql.append(")").append(sinkExtend);

            //编译SQL
            System.out.println("建表语句为：" + createTableSql);
            preparedStatement = connection.prepareStatement(createTableSql.toString());

            //执行SQL 建表
            preparedStatement.execute();

            // 释放资源

        } catch (SQLException e) {
            throw new RuntimeException("建表失败:"+sinkTable);
        }finally {
            if (preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

        // 1 获取广播流的配置数据
//        System.out.println(jsonObject);
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String table = jsonObject.getString("table");
        TableProcess tableProcess = broadcastState.get(table);

        if (tableProcess != null) {
            // 2 过滤字段 filterColumn
            filterColumn(jsonObject.getJSONObject("data"), tableProcess.getSinkColumns());

            // 3 补充SinkTable并写出到流中
            jsonObject.put("sinkTable",tableProcess.getSinkTable());
            collector.collect(jsonObject);
            
        }else {
            System.out.println("找不到对应的key："+ table);

        }
        }

    /**
     *
     * @param data
     * @param sinkColumns
     */
    private void filterColumn(JSONObject data, String sinkColumns) {

        // 切分sinkColumns
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);
        // 3 补充SinkTable字段输出

            Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !columnList.contains(next.getKey()));
    }
}