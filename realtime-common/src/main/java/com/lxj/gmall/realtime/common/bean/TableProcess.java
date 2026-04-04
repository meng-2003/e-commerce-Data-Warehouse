package com.lxj.gmall.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
/**
 * @author Laoxingjie
 * @description 配置表实体类
 * @create 2026/4/4 14:01
 **/

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcess {
    // 来源表
    String sourceTable;
    // 来源操作类型
    String sourceType;
    // 输出表
    String sinkTable;
    // 输出类型 dwd | dim
    String sinkType;
    // 数据到 hbase 的列族
    String sinkFamily;
    // 输出字段
    String sinkColumns;
    // sink到 hbase 的时候的主键字段
    String sinkRowKey;
    // 建表扩展（一些额外参数可以在此配置）
    String sinkExtend;
    String op; // 配置表操作: c r u d
}