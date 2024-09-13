package com.atguigu.educate.realtime.common.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateFormatUtil {
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter dtfForPartition = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter dtfFull = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 2023-07-05 01:01:01 转成 ms 值
     *
     * @param dateTime
     * @return
     */
    public static Long dateTimeToTs(String dateTime) {
        LocalDateTime localDateTime = LocalDateTime.parse(dateTime, dtfFull);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    /**
     * 把毫秒值转成 年月日:  2023-07-05
     *
     * @param ts
     * @return
     */
    public static String tsToDate(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }

    /**
     * 把毫秒值转成 年月日时分秒:  2023-07-05 01:01:01
     *
     * @param ts
     * @return
     */
    public static String tsToDateTime(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtfFull.format(localDateTime);
    }

    public static String tsToDateForPartition(long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtfForPartition.format(localDateTime);
    }

    /**
     * 把 年月日转成 ts
     *
     * @param date
     * @return
     */
    public static long dateToTs(String date) {
        return dateTimeToTs(date + " 00:00:00");
    }


    /**
     * 把 求两个日期的天数差
     *
     * @param date1,date2
     * @return
     */
    public static long getDateCutOfDays(String date1, String date2) {
        Long ts1=dateTimeToTs(date1 + " 00:00:00");
        Long ts2=dateTimeToTs(date2 + " 00:00:00");

        // 计算时间戳差值
        long diffInMillis = Math.abs(ts1 - ts2);

        // 将差值转换为天数（1天 = 86400000毫秒）
        return diffInMillis / (24 * 60 * 60 * 1000);

    }


}