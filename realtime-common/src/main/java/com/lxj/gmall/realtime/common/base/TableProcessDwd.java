package com.lxj.gmall.realtime.common.base;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
/**
 * @author Laoxingjie
 * @description 配置表
 * @create 2026/4/9 21:16
 **/

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcessDwd {
    // 来源表名，作为数据源的业务数据表名
    String sourceTable;

    // 来源类型，要筛选的原始数据操作类型
    String sourceType;

    // 目标表名，作为数据目的地的Kafka主题
    String sinkTable;

    // 输出字段，写出到Kafka的字段
    String sinkColumns;

    // 配置表操作类型
    String op;
}
