package com.lxj.gmall.realtime.dws;

import com.lxj.gmall.realtime.dws.function.KwSplit;
import com.lxj.gmall.realtime.common.base.BaseSQLApp;
import com.lxj.gmall.realtime.common.constant.Constant;
import com.lxj.gmall.realtime.common.util.FlinkSourceUtil;
import com.lxj.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Laoxingjie
 * @description 流量域搜索关键词粒度页面浏览各窗口汇总表
 * @create 2026/4/10 15:48
 **/
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(
                10021,
                4,
                Constant.TOPIC_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW
        );
    }

//    @Override
    public void handle2(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        try {
            KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(
                    Constant.TOPIC_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW,
                    Constant.TOPIC_DWD_TRAFFIC_PAGE
            );

            env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "test1")
                    .print();
            //3> {"common":{"ar":"18","uid":"467","os":"Android 13.0","ch":"web","is_new":"1","md":"Redmi k50","mid":"mid_331","vc":"v2.1.134","ba":"Redmi","sid":"f2134305-8c2e-4d97-b5f2-5d95bad23f63"},"page":{"from_pos_seq":1,"page_id":"good_detail","item":"1","during_time":6489,"item_type":"sku_id","last_page_id":"home","from_pos_id":1},"ts":1654681524000}
            //4> {"common":{"ar":"9","uid":"198","os":"Android 13.0","ch":"wandoujia","is_new":"1","md":"vivo x90","mid":"mid_190","vc":"v2.1.132","ba":"vivo","sid":"b28e631a-1284-4201-922e-8d693a8e0c56"},"page":{"page_id":"payment","item":"7352","during_time":12684,"item_type":"order_id","last_page_id":"order"},"ts":1654681524000}
            //1> {"common":{"ar":"19","uid":"376","os":"Android 13.0","ch":"xiaomi","is_new":"0","md":"SAMSUNG Galaxy s22","mid":"mid_207","vc":"v2.1.134","ba":"SAMSUNG","sid":"4edeb868-d7b3-4015-b886-c3a46ac82454"},"page":{"from_pos_seq":1,"page_id":"good_detail","item":"15","during_time":10278,"item_type":"sku_id","last_page_id":"good_list","from_pos_id":10},"ts":1654681524000}
            //4> {"common":{"ar":"34","uid":"223","os":"Android 13.0","ch":"xiaomi","is_new":"0","md":"SAMSUNG Galaxy s22","mid":"mid_146","vc":"v2.1.132","ba":"SAMSUNG","sid":"34649158-217a-4244-90f7-9b8002a78ae8"},"page":{"page_id":"payment","item":"7354","during_time":7171,"item_type":"order_id","last_page_id":"order"},"ts":1654681524000}
            //2> {"common":{"ar":"9","uid":"823","os":"iOS 13.2.9","ch":"Appstore","is_new":"0","md":"iPhone 14","mid":"mid_263","vc":"v2.1.134","ba":"iPhone","sid":"86f6a0f3-ab1e-4849-b51f-d781c5f6ef1a"},"page":{"from_pos_seq":0,"page_id":"good_detail","item":"9","during_time":12588,"item_type":"sku_id","last_page_id":"good_list","from_pos_id":10},"ts":1654681524000}
            //1> {"common":{"ar":"34","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone 14","mid":"mid_412","vc":"v2.1.134","ba":"iPhone","sid":"6189f7c9-e332-4230-924e-f43077ff5275"},"page":{"page_id":"home","refer_id":"4","during_time":14437},"ts":1654681524000}
            // 执行任务
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 读取页面日志
        tEnv.executeSql(
                "create table page_log(" +
                        "page map<string, string>," +
                        "ts bigint," +
                        "et as to_timestamp_ltz(ts, 3)," +
                        "watermark for et as et - interval '5' second" +
                        ")"
                + SQLUtil.getKafkaDDLSource(Constant.TOPIC_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW, Constant.TOPIC_DWD_TRAFFIC_PAGE)
        );
//        tEnv.executeSql("select * from page_log").print();
        //+----+--------------------------------+----------------------+-------------------------+
        //| op |                           page |                   ts |                      et |
        //+----+--------------------------------+----------------------+-------------------------+
        //| +I | {item_type=sku_id, item=28,... |        1654681881000 | 2022-06-08 17:51:21.000 |
        //| +I | {item_type=sku_id, item=18,... |        1654681882000 | 2022-06-08 17:51:22.000 |
        //| +I | {item_type=sku_id, item=12,... |        1654681881000 | 2022-06-08 17:51:21.000 |
        //| +I | {during_time=7526, page_id=... |         1654681882000 | 2022-06-08 17:51:22.000 |
        //| +I | {during_time=6961, page_id=... |        1654681882000 | 2022-06-08 17:51:22.000 |
        //| +I | {during_time=11223, page_id... |        1654681882000 | 2022-06-08 17:51:22.000 |
        // 2. 读取搜索关键词
        Table kwTable = tEnv.sqlQuery("select " +
                "page['item'] kw, " +
                "et " +
                "from page_log " +
                "where ( page['last_page_id'] ='search' " +
                "        or page['last_page_id'] ='home' " +
                "       )" +
                "and page['item_type']='keyword' " +
                "and page['item'] is not null ");
        tEnv.createTemporaryView("kw_table", kwTable);

        // 3. 自定义分词函数
        tEnv.createTemporaryFunction("kw_split", KwSplit.class);

        Table keywordTable = tEnv.sqlQuery("select " +
                " keyword, " +
                " et " +
                "from kw_table " +
                "join lateral table(kw_split(kw)) on true " +
                "where keyword is not null and keyword <> '' ");
        tEnv.createTemporaryView("keyword_table", keywordTable);
//        tEnv.executeSql("select * from keyword_table").print();
//        +----+--------------------------------+-------------------------+
//                | op |                        keyword |                      et |
//                +----+--------------------------------+-------------------------+
//                | +I |                           前端 | 2022-06-08 19:01:05.000 |
//                | +I |                           java | 2022-06-08 19:01:06.000 |
//                | +I |                         多线程 | 2022-06-08 19:01:05.000 |
//                | +I |                          flink | 2022-06-08 19:01:05.000 |
//                | +I |                           前端 | 2022-06-08 19:01:05.000 |
//                | +I |                           前端 | 2022-06-08 19:01:05.000 |
//                | +I |                           前端 | 2022-06-08 19:01:06.000 |
//                | +I |                           前端 | 2022-06-08 19:01:06.000 |
//                | +I |                         python | 2022-06-08 19:01:05.000 |

        // 3. 开窗聚和 tvf
        Table result = tEnv.sqlQuery("select " +
                " date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt, " +
                " date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt, " +
                " date_format(window_start, 'yyyyMMdd') cur_date, " +
                " keyword," +
                " count(*) keyword_count " +
                "from table( tumble(table keyword_table, descriptor(et), interval '5' second ) ) " +
                "group by window_start, window_end, keyword ");
        // 5. 写出到 doris 中
        tEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(" +
                "  stt string, " +  // 2023-07-11 14:14:14
                "  edt string, " +
                "  cur_date string, " +
                "  keyword string, " +
                "  keyword_count bigint " +
                ")with(" +
                " 'connector' = 'doris'," +
                " 'fenodes' = '" + Constant.DORIS_FE_NODES + "'," +
                "  'table.identifier' = '" + Constant.DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window'," +
                "  'username' = 'root'," +
                "  'password' = '000000', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
                "  'sink.properties.read_json_by_line' = 'true' " +
                ")");
        result.executeInsert("dws_traffic_source_keyword_page_view_window");
    }
}
