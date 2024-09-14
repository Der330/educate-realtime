package com.atguigu.ads.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @Author: 刘大大
 * @CreateTime: 2024/9/14  15:22
 */

@Data
@AllArgsConstructor
public class TrafficSourceStats {

    //来源
    String source;
    //会话持续时长
    BigDecimal duration;
    //独立访客数
    Integer uvCt;
}
