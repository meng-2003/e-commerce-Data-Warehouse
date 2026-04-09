package com.lxj.gmall.realtime.common.util;

import com.lxj.gmall.realtime.common.constant.Constant;

/**
 * @author Laoxingjie
 * @description 用于FlinkSQL的kafka连接工具类
 * @create 2026/4/8 17:28
 **/
public class SQLUtil {
    public static String getKafkaDDLSource(String groupId, String topic) {
        return "with(" +
                "  'connector' = 'kafka'," +
                "  'properties.group.id' = '" + groupId + "'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'json.ignore-parse-errors' = 'true'," + // 当 json 解析失败的时候,忽略这条数据
                "  'format' = 'json' " +
                ")";
    }

    public static String getKafkaDDLSink(String topic) {
        return "with(" +
                "  'connector' = 'kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'format' = 'json' " +
                ")";
    }
    public static String getUpsertKafkaDDL(String topic) {
        return "with(" +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'key.json.ignore-parse-errors' = 'true'," +
                "  'value.json.ignore-parse-errors' = 'true'," +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }

}
