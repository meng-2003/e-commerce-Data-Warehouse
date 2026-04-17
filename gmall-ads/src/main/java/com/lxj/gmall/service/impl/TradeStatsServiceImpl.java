package com.lxj.gmall.service.impl;

import com.lxj.gmall.bean.TradeProvinceOrderAmount;
import com.lxj.gmall.mapper.TradeStatsMapper;
import com.lxj.gmall.service.TradeStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author lxj
 * @date 2026/4/14
 * 交易域统计service接口实现类
 */
@Service
public class TradeStatsServiceImpl implements TradeStatsService {
    @Autowired
    private TradeStatsMapper tradeStatsMapper;
    @Override
    public BigDecimal getGMV(Integer date) {
        return tradeStatsMapper.selectGMV(date);
    }

    @Override
    public List<TradeProvinceOrderAmount> getProvinceAmount(Integer date) {
        return tradeStatsMapper.selectProvinceAmount(date);
    }
}
