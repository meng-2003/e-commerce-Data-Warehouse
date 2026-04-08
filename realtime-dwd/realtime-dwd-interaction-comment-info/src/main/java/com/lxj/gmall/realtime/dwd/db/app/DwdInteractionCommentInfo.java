package com.lxj.gmall.realtime.dwd.db.app;

import com.lxj.gmall.realtime.common.base.BaseSQLApp;
import com.lxj.gmall.realtime.common.constant.Constant;
import com.lxj.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Laoxingjie
 * @description 互动域评论事务事实表
 * @create 2026/4/8 19:03
 **/
public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(
                10012,
                4,
                Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1.通过ddl方式建立动态表： 从 topic_db 读取数据（source),下面这个传参是消费者组
        readOdsDb(tEnv, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
        // 2.过滤出评论表数据
        //Flink SQL 解析【JSON 嵌套字段】的专用语法
        Table commentInfo = tEnv.sqlQuery(
                "select\n" +
                        "\t`data`['id'] id,\n" +
                        "\t`data`['user_id'] user_id,\n" +
                        "\t`data`['sku_id'] sku_id,\n" +
                        "\t`data`['appraise'] appraise,\n" +
                        "\t`data`['comment_txt'] comment_txt,\n" +
                        "\t`data`['create_time'] comment_time,\n" +
                        "\tts,\n" +
                        "\tpt\n" +
                        "from topic_db\n" +
                        "where `database` = 'gmall'\n" +
                        "and `table` = 'comment_info'\n" +
                        "and `type` = 'insert'"
        );
        tEnv.createTemporaryView("comment_info", commentInfo);
        //tEnv.executeSql("select * from comment_info").print();
        //| op |                             id |                        user_id |                         sku_id |                       appraise |                    comment_txt |                   comment_time |                   ts |                      pt |
        //+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+----------------------+-------------------------+
        //| +I |                            275 |                            166 |                             27 |                           1201 | 评论内容：26953716899378273... |            2022-06-08 19:40:52 |           1654774852 | 2026-04-08 19:40:51.891 |
        //| +I |                            276 |                            166 |                              3 |                           1201 | 评论内容：88726434877944273... |            2022-06-08 19:40:52 |           1654774852 | 2026-04-08 19:40:51.888 |
        //| +I |                            278 |                           1064 |                             25 |                           1201 | 评论内容：25982651146584799... |            2022-06-08 19:40:53 |           1654774853 | 2026-04-08 19:40:53.136 |
        //| +I |                            277 |                           1064 |                             11 |                           1201 | 评论内容：18355495891227242... |            2022-06-08 19:40:53 |           1654774853 | 2026-04-08 19:40:53.719 |


        //3.通过ddl方式建表： base_dic hbase 中的维度表（source)
        readBaseDic(tEnv);
        //4.事实表与维度表的 join: lookup join
        Table result = tEnv.sqlQuery(
                "select\n" +
                        "\tci.id,\n" +
                        "\tci.user_id,\n" +
                        "\tci.sku_id,\n" +
                        "\tci.appraise,\n" +
                        "\tdic.info.dic_name appraise_name,\n" +
                        "\tci.comment_txt,\n" +
                        "\tci.ts\n" +
                        "from comment_info ci\n" +
                        "join base_dic for system_time as of ci.pt as dic \n" +
                        "on ci.appraise = dic.dic_code"
        );
        // 5.通过ddl方式建表：与Kafka的topic管理（sink)
        tEnv.executeSql(
                "create table dwd_interaction_comment_info(\n" +
                        "\tid string,\n" +
                        "\tuser_id string,\n" +
                        "\tsku_id string,\n" +
                        "\tappraise string,\n" +
                        "\tappraise_name string,\n" +
                        "\tcomment_txt string,\n" +
                        "\tts bigint\n" +
                        ")" + SQLUtil.getKafkaDDLSink(
                        Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO
                )
        );
        // 6.把join的结果写入到sink表
        result.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
}

