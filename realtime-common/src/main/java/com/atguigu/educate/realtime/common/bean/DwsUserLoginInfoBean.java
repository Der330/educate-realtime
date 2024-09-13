package com.atguigu.educate.realtime.common.bean;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class DwsUserLoginInfoBean {
    String uid;
    String loginDate;
    Long ts;
    Long isNewCount;
    Long lastDateCut;
}
