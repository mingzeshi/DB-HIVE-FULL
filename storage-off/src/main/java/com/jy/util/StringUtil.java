package com.jy.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.MessageFormat;

public class StringUtil {

    public static int toInteger(String value, int defaultValue) {
        try {
            if ("".equals(trim(value))) {
                return defaultValue;
            }
            return Integer.parseInt(value);
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    public static int toInteger(String value) {
        return toInteger(value, 0);
    }

    public static long toLong(String value, long defaultValue) {
        try {
            if ("".equals(trim(value))) {
                return defaultValue;
            }
            return Long.parseLong(value);
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    public static long toLong(String value) {
        return toLong(value, 0l);
    }

    public static String trim(String value, String defaultValue) {
        if (value == null || "".equals(value)) return defaultValue;
        else return value.trim();
    }


    public static String trim(String value) {
        return trim(value, "");
    }
    
	public static double toDouble(String value, double defaultValue) {
		try {
			if ("".equals(trim(value))) {
				return defaultValue;
			}
			return Double.parseDouble(value);
		} catch (Exception ex) {
			return defaultValue;
		}
	}
	
	public static double toDouble(String value) {
		return toDouble(value, 0d);
	}

    public static BigDecimal div(String value1, String value2) {
        return new BigDecimal(value1).divide(new BigDecimal(value2), 2, BigDecimal.ROUND_HALF_UP);
    }

    public static BigDecimal div(long value1, int value2) {
        return new BigDecimal(value1).divide(new BigDecimal(value2), 2, BigDecimal.ROUND_HALF_UP);
    }

    public static BigDecimal div(double value1, double value2,int n) {
        return new BigDecimal(value1).divide(new BigDecimal(value2), n, BigDecimal.ROUND_HALF_UP);
    }

    public static BigDecimal div(String value1, String value2,int n) {
        return new BigDecimal(value1).divide(new BigDecimal(value2), n, BigDecimal.ROUND_HALF_UP);
    }

    public static String formatMessage(String pattern,Object... arr){
    	return MessageFormat.format(pattern, arr);
    }
    
	//保留n位小数
	public static double formatDecimal(double value) {
		return Double.parseDouble(new DecimalFormat("###.00").format(value));
	}

    public static String printStackTrace(Throwable t){
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw, true));
        return sw.getBuffer().toString();
    }

}



