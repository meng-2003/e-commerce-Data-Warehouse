package com.lxj.gmall.service;

import com.lxj.gmall.bean.TradeProvinceOrderAmount;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author lxj
 * @date 2026/4/14
 * 交易域统计service接口
 */
public interface TradeStatsService {
    //获取某天总交易额
    BigDecimal getGMV(Integer date);

    //获取某天各个省份交易额
    List<TradeProvinceOrderAmount> getProvinceAmount(Integer date);
}
