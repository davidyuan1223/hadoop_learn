package org.apache.hadoop.util;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

@InterfaceAudience.LimitedPrivate({"HDFS","MapReduce"})
@InterfaceStability.Unstable
/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/22
 **/
public final class Time {
    private static final long NANOSECONDS_PER_MILLISECOND=1000000;
    private static final TimeZone UTC_ZONE=TimeZone.getTimeZone("UTC");
    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT=new ThreadLocal<SimpleDateFormat>(){
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSSZ");
        }
    };
    public static long now(){
        return System.currentTimeMillis();
    }
    public static long monotonicNow(){
        return System.nanoTime()/NANOSECONDS_PER_MILLISECOND;
    }
    public static long monotonicNowNanos(){
        return System.nanoTime();
    }
    public static String formatTime(long millis){
        return DATE_FORMAT.get().format(millis);
    }
    public static long getUtcTime(){
        return Calendar.getInstance(UTC_ZONE).getTimeInMillis();
    }
}
