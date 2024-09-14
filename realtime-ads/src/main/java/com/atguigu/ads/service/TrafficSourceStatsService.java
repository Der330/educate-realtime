package com.atguigu.ads.service;

import com.atguigu.ads.bean.TrafficSourceStats;

import java.util.List;

public interface TrafficSourceStatsService {
    List<TrafficSourceStats> getTrafficSourceStats(Integer date);
}
