package com.lxj.gmall.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author lxj
 * @date 2026/4/14
 */
@Data
@AllArgsConstructor
public class TrafficUvCt {
    // 渠道
    String ch;
    // 独立访客数
    Integer uvCt;
}
