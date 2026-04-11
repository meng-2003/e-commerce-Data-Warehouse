package com.lxj.gmall.realtime.dwd.db.app;

import com.lxj.gmall.realtime.common.base.BaseSQLApp;
import com.lxj.gmall.realtime.common.constant.Constant;
import com.lxj.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Laoxingjie
 * @description 交易域加购事务事实表
 * @create 2026/4/8 22:02
 **/
public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(
                10013,
                4,
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1.读取topic_db数据
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_CART_ADD);
//        tEnv.executeSql("select * from topic_db").print();
        // 2.过滤出加购数据
        Table cartAdd = tEnv.sqlQuery(
                "SELECT\n" +
                        "    `data`['id'] id,\n" +
                        "    `data`['user_id'] user_id,\n" +
                        "    `data`['sku_id'] sku_id,\n" +
                        "    IF(`type`='insert',\n" +
                        "        CAST(`data`['sku_num'] AS INT),\n" +
                        "        CAST(`data`['sku_num'] AS INT) - CAST(`old`['sku_num'] AS INT)\n" +
                        "    ) sku_num,\n" +
                        "    ts\n" +
                        "FROM topic_db\n" +
                        "WHERE `database`='gmall'\n" +
                        "  AND `table`='cart_info'\n" +
                        "  AND (\n" +
                        "        `type`='insert'\n" +
                        "        OR (\n" +
                        "            `type`='update'\n" +
                        "            AND `old`['sku_num'] IS NOT NULL\n" +
                        "            AND CAST(`data`['sku_num'] AS INT) > CAST(`old`['sku_num'] AS INT)\n" +
                        "           )\n" +
                        "      );"
        );
        tEnv.executeSql(
                "create table dwd_trade_cart_add(" +
                        "   id string, " +
                        "   user_id string," +
                        "   sku_id string," +
                        "   sku_num int, " +
                        "   ts  bigint " +
                        ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_CART_ADD)
        );
        cartAdd.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}
