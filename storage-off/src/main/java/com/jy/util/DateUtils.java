package com.jy.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 日期时间工具类
 * @author Administrator
 *
 */
public class DateUtils {
	
	public static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
	public static final SimpleDateFormat DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd");
	public static final SimpleDateFormat NUMBER_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");

	/**
	 * 计算时间差值（单位为秒）
	 * @param time1 时间1
	 * @param time2 时间2
	 * @return 差值
	 */
	public static int minus(String time1, String time2) {
		try {
			Date datetime1 = TIME_FORMAT.parse(time1);
			Date datetime2 = TIME_FORMAT.parse(time2);
			
			long millisecond = datetime1.getTime() - datetime2.getTime();
			
			return Integer.valueOf(String.valueOf(millisecond / 1000));  
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	/**
	 * 获取年月日和小时
	 * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
	 * @return 结果（yyyy-MM-dd_HH）
	 */
	public static String getDateHour(String datetime) {
		String date = datetime.split(" ")[0];
		String hourMinuteSecond = datetime.split(" ")[1];
		String hour = hourMinuteSecond.split(":")[0];
		return date + "_" + hour;
	}

	/**
	 * 获取当天日期（yyyy-MM-dd）
	 * @return 当天日期
	 */
	public static String getTodayDate() {
		return DATE_FORMAT.format(new Date());  
	}
	
	/**
	 * 获取昨天的日期（yyyy-MM-dd）
	 * @return 昨天的日期
	 */
	public static String getYesterdayDate() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());  
		cal.add(Calendar.DAY_OF_YEAR, -1);  
		
		Date date = cal.getTime();
		
		return DATE_FORMAT.format(date);
	}
	
	/**
	 * 格式化日期（yyyy-MM-dd）
	 * @param date Date对象
	 * @return 格式化后的日期
	 */
	public static String formatDate(Date date) {
		return DATE_FORMAT.format(date);
	}
	
	/**
	 * 格式化时间（yyyy-MM-dd HH:mm:ss）
	 * @param date Date对象
	 * @return 格式化后的时间
	 */
	public static String formatTime(Date date) {
		return TIME_FORMAT.format(date);
	}

	/**
	 * 格式化时间（yyyyMMddHHmmss）
	 * @param date Date对象
	 * @return 格式化后的时间
	 */
	public static String formatNumber(Date date) {
		return NUMBER_FORMAT.format(date);
	}
	
	/**
	 * 解析时间字符串
	 * @param time 时间字符串 
	 * @return Date
	 */
	public static Date parseTime(String time) {
		try {
			return TIME_FORMAT.parse(time);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 格式化日期key
	 * @param date
	 * @return
	 */
	public static String formatDateKey(Date date) {
		return DATEKEY_FORMAT.format(date);
	}

	/**
	 * 获取整10分钟
	 * @param min
	 * @return
	 */
	public static String getMIN10(String min) { return min.substring(0, 1) + "0"; }
}