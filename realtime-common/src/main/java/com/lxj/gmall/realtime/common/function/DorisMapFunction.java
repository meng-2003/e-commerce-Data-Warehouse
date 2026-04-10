package com.lxj.gmall.realtime.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author Laoxingjie
 * @description 用于完成小驼峰至下划线命名的转换
 * @create 2026/4/10 22:48
 **/
public class DorisMapFunction<T> implements MapFunction<T, String> {

    @Override
    public String map(T bean) throws Exception {
        SerializeConfig conf = new SerializeConfig();
        conf.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSON.toJSONString(bean, conf);
    }
}
