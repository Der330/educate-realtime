package com.atguigu.ads.controller;

import com.atguigu.ads.bean.TrafficSourceStats;
import com.atguigu.ads.service.TrafficSourceStatsService;
import com.atguigu.ads.service.impl.TrafficSourceStatsServiceImpl;
import com.atguigu.ads.util.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: 刘大大
 * @CreateTime: 2024/9/14  15:36
 */

@RestController
public class TrafficStatsController {
    @Autowired
    TrafficSourceStatsServiceImpl trafficSourceStatsServiceImpl;

    @RequestMapping("/ch")
    public String getChUvCt(
            @RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0){
            date = DateFormatUtil.now();
        }

        List<TrafficSourceStats> trafficUvCtList =trafficSourceStatsServiceImpl.getTrafficSourceStats(date);

        List uvCtList = new ArrayList();

        for (TrafficSourceStats trafficUvCt : trafficUvCtList) {
            uvCtList.add(trafficUvCt.getUvCt());
        }

        String json = "{\"status\": 0,\"data\": [\""+ StringUtils.join(date,"\",\"")+"\"],\"value\": 25}}";
        return json;
    }
}
