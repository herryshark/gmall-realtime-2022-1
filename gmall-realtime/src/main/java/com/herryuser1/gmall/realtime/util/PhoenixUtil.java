package com.herryuser1.gmall.realtime.util;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.herryuser1.gmall.realtime.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class PhoenixUtil {
    /**
     * @param connection Phoenix连接
     * @param sinkTable  表名
     * @param data       数据
     */

    public static void upsertValues(DruidPooledConnection connection, String sinkTable, JSONObject data) throws SQLException {

        // 1 拼接SQL语句，从原先JSON格式的kv对改写为db.tn(k1,k2,k3...) values(v1,v2,v3...)的格式
        Set<String> columns = data.keySet();//列名
        Collection<Object> values = data.values();//值
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(columns, ",")+") values ('"+
                StringUtils.join(values,"','")+"')";

        // 2 预编译SQL
//        System.out.println(sql);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        // 3 执行
        preparedStatement.execute();
        connection.commit();

        // 4 释放资源
        preparedStatement.close();

    }
}
