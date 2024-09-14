package com.atguigu.ads.util;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;

public class DateFormatUtil {
    //获取当前日期的整数形式
    public static Integer now(){
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }
}