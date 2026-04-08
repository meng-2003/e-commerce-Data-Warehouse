package com.lxj.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.lxj.gmall.realtime.common.base.BaseApp;
import com.lxj.gmall.realtime.common.constant.Constant;
import com.lxj.gmall.realtime.common.util.DateFormatUtil;
import com.lxj.gmall.realtime.common.util.FlinkSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;

/**
 * @author Laoxingjie
 * @description 数据域未加工的事务事实表（日志分流）
 * @create 2026/4/8 15:07
 **/
@Slf4j
public class DwdBaseLog extends BaseApp{

    private final String START = "start";
    private final String ERR = "err";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private final String PAGE = "page";

    public static void main(String[] args) {
        new DwdBaseLog().start(
                10011,
                4,
                "dwd_base_log",
                Constant.TOPIC_LOG
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //1.etl
        SingleOutputStreamOperator<JSONObject> etlstream = etl(stream);
        // {"common":{"ar":"16","uid":"157","os":"Android 13.0","ch":"wandoujia","is_new":"1","md":"OPPO Remo8","mid":"mid_397","vc":"v2.1.132","ba":"OPPO","sid":"1c2e3a2d-9f0f-45a2-b172-bc6ecdc149b1"},"page":{"page_id":"order","item":"35","during_time":15218,"item_type":"sku_ids","last_page_id":"good_detail"},"ts":1654674704000}

        //2.纠正新老客户
        SingleOutputStreamOperator<JSONObject> validatedStream = validateNewOrOld(etlstream);

        //3.分流
        HashMap<String, DataStream<JSONObject>> streams = splitStream(validatedStream);

        //4.不同的流写出到不同的topic
        writeToKafka(streams);
    }

    private void writeToKafka(HashMap<String, DataStream<JSONObject>> streams) {
        streams
                .get(START)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));

        streams
                .get(ERR)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));

        streams
                .get(DISPLAY)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));

        streams
                .get(PAGE)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));

        streams
                .get(ACTION)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }

    private HashMap<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> validatedStream) {
        /*
        （3）分流思路
① 所有日志数据都可能拥有 err 字段，所有首先获取 err 字段，如果返回值不为 null 则将整条日志数据发送到错误侧输出流。然后删掉 JSONObject 中的 err 字段及对应值；
② 判断是否有 start 字段，如果有则说明数据为启动日志，将其发送到启动侧输出流；如果没有则说明为页面日志，进行下一步；
③ 页面日志必然有 page 字段、 common 字段和 ts 字段，获取它们的值，ts 封装为包装类 Long，其余两个字段的值封装为 JSONObject；
④ 判断是否有 displays 字段，如果有，将其值封装为 JSONArray，遍历该数组，依次获取每个元素（记为 display），封装为JSONObject。创建一个空的 JSONObject，将 display、common、page和 ts 添加到该对象中，获得处理好的曝光数据，发送到曝光侧输出流。动作日志的处理与曝光日志相同（注意：一条页面日志可能既有曝光数据又有动作数据，二者没有任何关系，因此曝光数据不为 null 时仍要对动作数据进行处理）；
⑤ 动作日志和曝光日志处理结束后删除displays和actions 字段，此时主流的 JSONObject 中只有 common 字段、 page 字段和 ts 字段，即为最终的页面日志。
处理结束后，页面日志数据位于主流，其余四种日志分别位于对应的侧输出流，将五条流的数据写入 Kafka 对应主题即可。

         */

        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display"){};
        OutputTag<JSONObject> actionTag = new OutputTag<JSONObject>("action"){};
        OutputTag<JSONObject> errTag = new OutputTag<JSONObject>("err"){};
        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>("page"){};
        SingleOutputStreamOperator<JSONObject> startStream = validatedStream.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject common = value.getJSONObject("common");
                Long ts = value.getLong("ts");

                //1.启动
                JSONObject start = value.getJSONObject("start");
                if (start != null) {
                    out.collect(value);
                }

                //2.曝光
                JSONArray displays = value.getJSONArray("displays");
                if (displays != null) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);
                        display.putAll(common);
                        display.put("ts", ts);
                        ctx.output(displayTag, display);
                    }
                    value.remove("displays");
                }

                // 3.活动
                JSONArray actions = value.getJSONArray("actions");
                if (actions != null) {
                    for (int i = 0; i < actions.size(); i++) {
                        JSONObject action = actions.getJSONObject(i);
                        action.putAll(common);
                        ctx.output(actionTag, action);
                    }
                    value.remove("actions");
                }


                // 4. err
                JSONObject err = value.getJSONObject("err");
                if (err != null) {
                    ctx.output(errTag, value);
                    value.remove("err");
                }

                // 5. 页面
                JSONObject page = value.getJSONObject("page");
                if (page != null) {
                    ctx.output(pageTag, value);
                }
            }
        });
        HashMap<String, DataStream<JSONObject>> streams = new HashMap<>();
        streams.put(START, startStream);
        streams.put(DISPLAY, startStream.getSideOutput(displayTag));
        streams.put(ERR, startStream.getSideOutput(errTag));
        streams.put(PAGE, startStream.getSideOutput(pageTag));
        streams.put(ACTION, startStream.getSideOutput(actionTag));

        return streams;
    }

    private SingleOutputStreamOperator<JSONObject> validateNewOrOld(SingleOutputStreamOperator<JSONObject> etlstream) {
        return etlstream.keyBy(obj -> obj.getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<String> firstVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject common = value.getJSONObject("common");
                        String isNew = common.getString("is_new");
                        Long ts = value.getLong("ts");
                        String today = DateFormatUtil.tsToDate(ts);

                        // 从状态中获取首次访问日志
                        String firstVisitDate = firstVisitDateState.value();

                        if ("1".equals(isNew)) {    //流中数据认为是首次访问
                            if (firstVisitDate == null) {
                                firstVisitDateState.update(today);
                            } else if (!today.equals(firstVisitDate)) {
                                common.put("is_new", "0");
                            }
                        } else {
                            if (firstVisitDate == null) {
                                firstVisitDateState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000));
                            }
                        }
                        out.collect(value);
                    }
                });
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                try {
                    JSON.parseObject(s);
                    return true;
                } catch (Exception e) {
                    log.error("日志格式不是正确的JSON格式：" + s);
                    return false;
                }
            }
        }).map(JSON::parseObject);
    }
}
