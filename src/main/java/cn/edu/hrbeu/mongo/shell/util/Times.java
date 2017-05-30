/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hrbeu.mongo.shell.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * Created by wu on 2017/5/25.
 */
public class Times {
    

    public static void wait(int millis) {
        sleep(millis);
    }

    public static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
        }
    }

    public static String getTimeString(String pattern, Date date) {
        return (new SimpleDateFormat(pattern).format(date));
    }

    public static String date2string(String pattern, Date date) {
        return (new SimpleDateFormat(pattern).format(date));
    }

    public static String getTimeString(String pattern, long t) {
        return (new SimpleDateFormat(pattern).format(new Date(t)));
    }

    public static String time2string(String pattern, long t) {
        return (new SimpleDateFormat(pattern).format(new Date(t)));
    }

    public static String getDateString(Date date) {
        return getTimeString("yyyy-MM-dd", date);
    }

    public static String getDateString(long t) {
        return getDateString(new Date(t));
    }

    public static String getDateString() {
        return getDateString(new Date());
    }

    public static String getDateTimeString(Date date) {
        return getTimeString("yyyy-MM-dd HH:mm:ss_SSS", date);
    }

    public static String getDateTimeString(long t) {
        return getDateTimeString(new Date(t));
    }

    public static String getDateTimeString() {
        return getDateTimeString(new Date());
    }

    public static String getTimeString(Long t) {
        return getTimeString(new Date(t));
    }

    public static String getTimeString(Date date) {
        return getTimeString("HH:mm:ss_SSS", date);
    }

    public static String getTimeString() {
        return getTimeString(new Date());
    }

    public static Date getDateFromString(String format, String s) {
        try {
            return new SimpleDateFormat(format).parse(s);
        } catch (ParseException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static Date date4string(String format, String s) {
        if (s == null) {
            return null;
        }
        try {
            return new SimpleDateFormat(format).parse(s);
        } catch (ParseException ex) {

        }
        return null;
    }

    public static long time4string(String format, String s) {
        Date date = date4string(format, s);
        if (date == null) {
            return 0;
        } else {
            return date.getTime();
        }
    }

    // 去一天的起始时间，即00:00
    public static Date floorDay(Date date) {
        GregorianCalendar gc = new GregorianCalendar();
        gc.setTime(date);
        if ((gc.get(GregorianCalendar.HOUR_OF_DAY) == 0) && (gc.get(GregorianCalendar.MINUTE) == 0)
                && (gc.get(GregorianCalendar.SECOND) == 0)) {
            return new Date(date.getTime());
        } else {
            Date date2 = new Date(date.getTime() - gc.get(GregorianCalendar.HOUR_OF_DAY) * 60 * 60
                    * 1000 - gc.get(GregorianCalendar.MINUTE) * 60 * 1000 - gc.get(GregorianCalendar.SECOND)
                    * 1000);
            return date2;
        }
    }

    public static Date floorDay(long t) {
        return floorDay(new Date(t));
    }

    public static final long DAY_TIME = 86400000L;
    public static final long HOUR_TIME = 3600000L;
    public static final long MIN_TIME = 60000L;

    // 获取当前起始天
    public static long thisDay(long t) {
        return t - ((t + (HOUR_TIME << 3)) % DAY_TIME);// - (HOUR_TIME << 3);
    }

    // 获取前一个起始天
    public static long prevDay(long t) {
        return thisDay(t) - DAY_TIME;
    }

    // 获取下一个起始天
    public static long nextDay(long t) {
        return thisDay(t) + DAY_TIME;
    }

    // 获取当前起始小时
    public static long thisHour(long t) {
        return t - (t % HOUR_TIME);
    }

    // 获取前一个起始小时
    public static long prevHour(long t) {
        return thisHour(t) - HOUR_TIME;
    }

    // 获取下一个起始小时
    public static long nextHour(long t) {
        return thisHour(t) + HOUR_TIME;
    }

    // 获取已align为尺度的地板时间
    public static long thisFloorTime(long t, long align) {
        return t - (t % align);
    }

    // 获取已align为尺度的前一个地板时间
    public static long prevFloorTime(long t, long align) {
        return thisFloorTime(t, align) - align;
    }

    // 获取已align为尺度的后一个地板时间

    public static long nextFloorTime(long t, long align) {
        return thisFloorTime(t, align) + align;
    }

    /**
     * 获取指定日期是星期几 参数为null时表示获取当前日期是星期几
     *
     * @param date
     * @return
     */
    public static String getWeekOfDate(Date date) {
        String[] weekOfDays = {"日", "一", "二", "三", "四", "五", "六"};
        Calendar calendar = Calendar.getInstance();
        if (date != null) {
            calendar.setTime(date);
        }
        int w = calendar.get(Calendar.DAY_OF_WEEK) - 1;
        if (w < 0) {
            w = 0;
        }
        return weekOfDays[w];
    }
    public static int getMonthOfDate(Date date) {
        Calendar calendar = Calendar.getInstance();
        if (date != null) {
            calendar.setTime(date);
        }
        int w = calendar.get(Calendar.DAY_OF_MONTH) - 1;
        return w;
    }
    
    /**
     * 获取指定日期是星期几 参数为null时表示获取当前日期是星期几
     *
     * @param t
     * @return
     */
    public static String getWeekOfDate(long t) {
        return getWeekOfDate(new Date(t));
    }
    
    public static String getWeekOfDate() {
        return getWeekOfDate(new Date());
    }

    public static void main(String[] args) {
//        System.err.println(new Date(thisDay(System.currentTimeMillis())));
//        System.err.println(new Date(nextHour(System.currentTimeMillis())));
//        System.err.println(new Date(prevFloorTime(System.currentTimeMillis(), 5 * MIN_TIME)));
//        System.err.println(new Date(thisFloorTime(System.currentTimeMillis(), 5 * MIN_TIME)));
//        System.err.println(new Date(nextFloorTime(System.currentTimeMillis(), 5 * MIN_TIME)));
        
        
        System.err.println(new Date(1442980800000L));
        
        System.err.println("value is " + (-101 / 100));
    }
    
    
}
