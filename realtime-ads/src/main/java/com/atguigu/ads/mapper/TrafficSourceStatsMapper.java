package com.atguigu.ads.mapper;

import com.atguigu.ads.bean.TrafficSourceStats;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;
@Mapper
public interface TrafficSourceStatsMapper {
    @Select("SELECT sum(uv_ct) uv_ct,\n" +
            "AVG(dur_sum) dur_time,\n" +
            "ch \n" +
            "FROM dws_traffic_is_new_page_view_window\n" +
            "partition par#{date}\n" +
            "GROUP BY ch;\n")
    List<TrafficSourceStats> selectTrafficStats(Integer date);
}
