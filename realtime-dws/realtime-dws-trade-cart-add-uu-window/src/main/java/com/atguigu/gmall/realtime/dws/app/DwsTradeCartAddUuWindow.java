package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lxj.gmall.realtime.common.base.BaseApp;
import com.lxj.gmall.realtime.common.bean.CartAddUuBean;
import com.lxj.gmall.realtime.common.constant.Constant;
import com.lxj.gmall.realtime.common.function.DorisMapFunction;
import com.lxj.gmall.realtime.common.util.DateFormatUtil;
import com.lxj.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
/**
 * @author Laoxingjie
 * @description 交易域加购各窗口汇总表
 * @create 2026/4/11 15:12
 **/

public class DwsTradeCartAddUuWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeCartAddUuWindow().start(
                10026,
                4,
                "dws_trade_cart_add_uu_window",
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream
                .map(JSON::parseObject)
//                .print();
//        1> {"sku_num":1,"user_id":"964","sku_id":"31","id":"11179","ts":1654767740}
//        1> {"sku_num":1,"user_id":"1933","sku_id":"31","id":"11180","ts":1654767740}
//        2> {"sku_num":1,"user_id":"2632","sku_id":"5","id":"11181","ts":1654767740}
//        3> {"sku_num":1,"user_id":"92","sku_id":"19","id":"11182","ts":1654767740}

                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                                .withTimestampAssigner((obj, ts) -> obj.getLong("ts") * 1000)
                                .withIdleness(Duration.ofSeconds(120L))

                )
                .keyBy(obj -> obj.getString("user_id"))
                // 使用flink状态编程，判断是否为加购独立用户
                .process(new KeyedProcessFunction<String, JSONObject, CartAddUuBean>() {

                    private ValueState<String> lastCartAddDateState;

                    @Override
                    public void open(Configuration parameters) {
                        lastCartAddDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastCartAddDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject jsonObj,
                                               Context context,
                                               Collector<CartAddUuBean> out) throws Exception {
                        String lastCartAddDate = lastCartAddDateState.value();
                        long ts = jsonObj.getLong("ts") * 1000;
                        String today = DateFormatUtil.tsToDate(ts);

                        if (!today.equals(lastCartAddDate)) {
                            lastCartAddDateState.update(today);

                            out.collect(new CartAddUuBean("", "", "", 1L));
                        }

                    }
                })
//                .print();
//        3> CartAddUuBean(stt=, edt=, curDate=, cartAddUuCt=1)
//        4> CartAddUuBean(stt=, edt=, curDate=, cartAddUuCt=1)
//        3> CartAddUuBean(stt=, edt=, curDate=, cartAddUuCt=1)
//        4> CartAddUuBean(stt=, edt=, curDate=, cartAddUuCt=1)
//        1> CartAddUuBean(stt=, edt=, curDate=, cartAddUuCt=1)
//        2> CartAddUuBean(stt=, edt=, curDate=, cartAddUuCt=1)
//        2> CartAddUuBean(stt=, edt=, curDate=, cartAddUuCt=1)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<CartAddUuBean>() {
                            @Override
                            public CartAddUuBean reduce(CartAddUuBean value1,
                                                        CartAddUuBean value2) {
                                value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                            @Override
                            public void process(Context ctx,
                                                Iterable<CartAddUuBean> elements,
                                                Collector<CartAddUuBean> out) throws Exception {
                                CartAddUuBean bean = elements.iterator().next();
                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));

                                out.collect(bean);
                            }
                        }
                )
//                .print();
//        1> CartAddUuBean(stt=2022-06-09 17:44:10, edt=2022-06-09 17:44:15, curDate=20220609, cartAddUuCt=5)
//        2> CartAddUuBean(stt=2022-06-09 17:44:15, edt=2022-06-09 17:44:20, curDate=20220609, cartAddUuCt=56)

                .map(new DorisMapFunction<>())
//                .print();
//        1> {"cart_add_uu_ct":37,"cur_date":"20220609","edt":"2022-06-09 17:45:00","stt":"2022-06-09 17:44:55"}
//                注释了maxwell的# mock_date=2022-06-09
//        1> {"cart_add_uu_ct":49,"cur_date":"20260411","edt":"2026-04-11 19:46:05","stt":"2026-04-11 19:46:00"}
//        2> {"cart_add_uu_ct":59,"cur_date":"20260411","edt":"2026-04-11 19:46:10","stt":"2026-04-11 19:46:05"}
//        3> {"cart_add_uu_ct":4,"cur_date":"20260411","edt":"2026-04-11 19:46:15","stt":"2026-04-11 19:46:10"}

                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABASE + ".dws_trade_cart_add_uu_window", "dws_trade_cart_add_uu_window"));


    }
}
