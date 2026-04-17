package com.lxj.gmall.service.impl;

import com.lxj.gmall.bean.TrafficUvCt;
import com.lxj.gmall.mapper.TrafficStatsMapper;
import com.lxj.gmall.service.TrafficStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author lxj
 * @date 2026/4/14
 * 流量域统计service接口实现类
 */

@Service
public class TrafficStatsServiceImpl implements TrafficStatsService {
    @Autowired
    private TrafficStatsMapper trafficStatsMapper;

    @Override
    public List<TrafficUvCt> getChUvCt(Integer date, Integer limit) {
        return trafficStatsMapper.selectChUvCt(date,limit);
    }
}
