package com.lxj.gmall.service;

import com.lxj.gmall.bean.TrafficUvCt;

import java.util.List;

/**
 * @author lxj
 * @date 2026/4/14
 * 流量域统计service接口
 */
public interface TrafficStatsService {
    //获取某天各个渠道独立访客数
    List<TrafficUvCt> getChUvCt(Integer date,Integer limit);
}
