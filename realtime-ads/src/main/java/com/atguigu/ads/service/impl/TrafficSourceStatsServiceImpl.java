package com.atguigu.ads.service.impl;

import com.atguigu.ads.bean.TrafficSourceStats;

import com.atguigu.ads.mapper.TrafficSourceStatsMapper;
import com.atguigu.ads.service.TrafficSourceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author: 刘大大
 * @CreateTime: 2024/9/14  15:28
 */

@Service
public class TrafficSourceStatsServiceImpl implements TrafficSourceStatsService {
    @Autowired
    TrafficSourceStatsMapper trafficSourceStatsMapper;


    @Override
    public List<TrafficSourceStats> getTrafficSourceStats(Integer date) {
        return trafficSourceStatsMapper.selectTrafficStats(date);
    }
}
