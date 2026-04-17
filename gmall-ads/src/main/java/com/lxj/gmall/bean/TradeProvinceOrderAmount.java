package com.lxj.gmall.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author lxj
 * @date 2026/4/14
 */
@Data
@AllArgsConstructor
public class TradeProvinceOrderAmount {
    // 省份名称
    String provinceName;
    // 下单金额
    Double orderAmount;
}

