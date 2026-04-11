package com.lxj.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
/**
 * @author Laoxingjie
 * @description 模板接口
 * @create 2026/4/11 16:58
 **/

public interface DimFunction<T> {
    String getRowKey(T bean);
    String getTableName();
    void addDims(T bean, JSONObject dim);
}
