package com.lxj.gmall.realtime.dws.function;

import com.lxj.gmall.realtime.dws.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Set;

import static org.apache.flink.streaming.api.datastream.DataStreamUtils.collect;

/**
 * @author Laoxingjie
 * @description 分词函数
 * @create 2026/4/10 15:09
 **/
@FunctionHint(output = @DataTypeHint("row<keyword string>"))
public class KwSplit extends TableFunction<Row> {
    public void eval(String kw) {
        if (kw == null) {
            return;
        }
        // "华为手机白色手机"
        Set<String> keywords = IkUtil.split(kw);
        for (String keyword : keywords) {
            collect(Row.of(keyword));
        }
    }
}
