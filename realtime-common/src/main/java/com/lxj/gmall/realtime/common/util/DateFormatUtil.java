package com.lxj.gmall.realtime.common.util;


import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @author Laoxingjie
 * @description 日期转换工具类
 * @create 2026/4/8 15:05
 **/
public class DateFormatUtil {
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter dtfFull = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    //毫秒值转年月日：2026-04-08
    public static String tsToDate(Long ts) {
        Date date = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }
    //毫秒值转年月日：20260408
    public static String tsToDateForPartition(Long ts) {
        Date date = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return dtf2.format(localDateTime);
    }
    //毫秒值转年月日时分秒：2026-04-08 16：17：00
    public static String tsToDateTime(Long ts) {
        Date date = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return dtfFull.format(localDateTime);
    }

    //年月日时分秒转毫秒值
    public static Long dateTimeToTs(String dateTime) {
        LocalDateTime parse = LocalDateTime.parse(dateTime, dtfFull);
        return parse.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }
    //年月日转毫秒值
    public static Long dateToTs(String date) {
        return dateTimeToTs(date + " 00:00:00");
    }
}
