package com.atguigu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lxj.gmall.realtime.common.base.BaseApp;
import com.lxj.gmall.realtime.common.base.TableProcessDwd;
import com.lxj.gmall.realtime.common.constant.Constant;
import com.lxj.gmall.realtime.common.util.FlinkSinkUtil;
import com.lxj.gmall.realtime.common.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

/**
 * @author Laoxingjie
 * @description 提取四张事实表
 * 工具域优惠券领取事务事实表
 * 工具域优惠券使用（下单+支付）事务事实表
 * 互动域收藏商品事务事实表
 * 用户域用户注册事务事实表
 * @create 2026/4/9 21:23
 **/
@Slf4j
public class DwdBaseDb extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseDb().start(
                10019,
                4,
                "dwd_base_db",
                Constant.TOPIC_DB
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 对消费的数据, 做数据清洗
//        stream.print();
//        4> {"database":"gmall","table":"favor_info","type":"insert","ts":1654704598,"xid":225398,"commit":true,"data":{"id":7213,"user_id":367,"sku_id":8,"spu_id":3,"is_cancel":"0","create_time":"2022-06-08 00:09:58","operate_time":null}}
//        2> {"database":"gmall","table":"cart_info","type":"update","ts":1654704598,"xid":225385,"commit":true,"data":{"id":5682,"user_id":"1761","sku_id":22,"cart_price":39.00,"sku_num":1,"img_url":null,"sku_name":"十月稻田 长粒香大米 东北大米 东北香米 5kg","is_checked":null,"create_time":"2022-06-08 00:08:57","operate_time":"2022-06-08 00:09:58","is_ordered":1,"order_time":"2022-06-08 00:09:58"},"old":{"operate_time":null,"is_ordered":0,"order_time":null}}
//        2> {"database":"gmall","table":"payment_info","type":"update","ts":1654704598,"xid":225403,"commit":true,"data":{"id":6283,"out_trade_no":"986933477288817","order_id":6283,"user_id":766,"payment_type":"1102","trade_no":null,"total_amount":20015.00,"subject":"联想（Lenovo） 拯救者Y9000P 2022 16英寸游戏笔记本电脑 i9-12900H RTX3070Ti 钛晶灰等3件商品","payment_status":"1602","create_time":"2022-06-08 00:09:57","callback_time":"2022-06-08 00:09:58","callback_content":"callback xxxxxxx","operate_time":"2022-06-08 00:09:58"},"old":{"payment_status":"1601","callback_time":null,"callback_content":null,"operate_time":null}}
//        3> {"database":"gmall","table":"favor_info","type":"insert","ts":1654704598,"xid":225389,"commit":true,"data":{"id":7212,"user_id":1765,"sku_id":1,"spu_id":1,"is_cancel":"0","create_time":"2022-06-08 00:09:58","operate_time":null}}
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);

        // 2. 通过flink cdc 读取配置表的数据
        SingleOutputStreamOperator<TableProcessDwd> configStream = readTableProcess(env);

        // 3.数据流去 connect 配置流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> dataWithConfigStream = connect(configStream, etlStream);

        // 4.删除不要的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> resultStream = deleteNotNeedColumns(dataWithConfigStream);

        // 5. 写出到Kafka中
        writeToKafka(resultStream);
    }

    private static void writeToKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> resultStream) {
        resultStream.sinkTo(FlinkSinkUtil.getKafkaSink());
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> deleteNotNeedColumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> dataWithConfigStream) {
        return dataWithConfigStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDwd>, Tuple2<JSONObject, TableProcessDwd>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDwd> map(Tuple2<JSONObject, TableProcessDwd> dataWithConfig) throws Exception {
                JSONObject data = dataWithConfig.f0;
                ArrayList<String> columns = new ArrayList<>(Arrays.asList(dataWithConfig.f1.getSinkColumns().split(",")));

                data.keySet().removeIf(key -> !columns.contains(key));
                return dataWithConfig;
            }
        });
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> connect(SingleOutputStreamOperator<TableProcessDwd> configStream, SingleOutputStreamOperator<JSONObject> etlStream) {
        // 1. 把配置流作为广播流
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>("table_process_dwd", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastStream = configStream.broadcast(mapStateDescriptor);
        // 2. 数据流去connect 广播流
        return etlStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {
                    private HashMap<String, TableProcessDwd> map;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // open无法访问（托管状态的）算子状态（中的广播状态）
                        // 只有当前并行度才能读取到的原始状态
                        map = new HashMap<>();
                        // 1. 去mysql中查询table_process 表所有数据
                        Connection mysqlConn = JdbcUtil.getMysqlConnection();
                        List<TableProcessDwd> tableProcessDwdList = JdbcUtil.queryList(mysqlConn,
                                "select * from gmall_config.table_process_dwd",
                                TableProcessDwd.class,
                                true
                        );

                        for (TableProcessDwd tableProcessDwd : tableProcessDwdList) {
                            String key = getKey(tableProcessDwd.getSourceTable(), tableProcessDwd.getSourceType());
                            map.put(key, tableProcessDwd);
                        }
                        JdbcUtil.closeConnection(mysqlConn);
                    }

                    // 处理数据流中的数据: 从广播状态中读取配置信息
                    @Override
                    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                        ReadOnlyBroadcastState<String, TableProcessDwd> state = ctx.getBroadcastState(mapStateDescriptor);
                        String key = getKey(value.getString("table"), value.getString("type"));
                        TableProcessDwd tableProcessDwd = state.get(key);

                        if (tableProcessDwd == null) {
                            tableProcessDwd = map.get(key);
                            if (tableProcessDwd != null) {
                                log.info("在 map 中查找到 " + key);
                            }
                        } else {
                            log.info("在状态中查找到" + key);
                        }
                        if (tableProcessDwd != null) {
                            JSONObject data = value.getJSONObject("data");
                            out.collect(Tuple2.of(data, tableProcessDwd));
                        }
                    }

                    // 处理广播流中的数据: 把配置信息存入到广播状态中
                    @Override
                    public void processBroadcastElement(TableProcessDwd value, Context ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                        BroadcastState<String, TableProcessDwd> state = ctx.getBroadcastState(mapStateDescriptor);
                        String key = getKey(value.getSourceTable(), value.getSourceType());
                        if ("d".equals(value.getOp())) {
                            // 删除广播状态和原始状态
                            state.remove(key);
                            map.remove(key);
                        } else {
                            // 更新或添加状态
                            state.put(key, value);
                        }
                    }

                    private String getKey(String table, String type) {
                        return table + ":" + type;
                    }
                });
    }

    private static SingleOutputStreamOperator<TableProcessDwd> readTableProcess(StreamExecutionEnvironment env) {
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("gmall_config")   //设置获取的数据库，如果需要全部数据库，设置".*"
                .tableList("gmall_config.table_process_dwd")
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .jdbcProperties(props)
                .deserializer(new JsonDebeziumDeserializationSchema())  //反序列化器
                .startupOptions(StartupOptions.initial())
                .build();
        return env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "cdc-source")
                .setParallelism(1)
                .map(new MapFunction<String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String value) throws Exception {
                        JSONObject obj = JSON.parseObject(value);
                        String op = obj.getString("op");
                        TableProcessDwd tp;
                        if ("d".equals(op)) {
                            tp = obj.getObject("before", TableProcessDwd.class);
                        } else {
                            tp = obj.getObject("after", TableProcessDwd.class);
                        }
                        tp.setOp(op);
                        return tp;
                    }
                }).setParallelism(1);
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                try {
                    JSONObject obj = JSON.parseObject(s);
                    String db = obj.getString("database");
                    String type = obj.getString("type");
                    String data = obj.getString("data");

                    return "gmall".equals(db)
                            && ("insert".equals(type)
                            || "update".equals(type))
                            && data != null
                            && data.length() > 2;
                } catch (Exception e) {
                    log.warn("不是正确的 json 格式的数据： " + s);
                    return false;
                }
            }
        }).map(JSON::parseObject);
    }
}
