package org.frozen.test;

import org.frozen.constant.Constants;
import org.frozen.util.CoderUtil;
import org.frozen.util.DateUtils;

import java.util.Date;

import org.apache.commons.lang.StringUtils;

public class StringTest {

    public static void main(String[] args) {
        /*
        String str = "insert";

        System.out.println(str.equalsIgnoreCase("INSERT"));
        System.out.println(str.equals("INSERT"));
        */

//        String str = "a\tb\tc\td\te\t";
//
//        System.out.print("-" + str.substring(0, str.length() - 1) + "-");

        try {

//            System.out.println(CoderUtil.decoder("%E4%BF%A1%E7%94%A8%E5%8D%A1%E8%BF%98%E6%AC%BE"));
//            System.out.println(CoderUtil.decoder("%E5%B9%B6%E5%85%A8%E9%83%A8%E9%81%B5%E5%BE%AA%E5%8E%9F%E6%9C%89%E5%8D%8F%E8%AE%AE%E5%8F%91%E5%B8%83%EF%BC%8C%E8%91%97%E4%BD%9C%E6%9D%83%E5%BD%92%E5%B1%9E%E5%8E%9F%E4%BD%9C%E8%80%85%E6%88%96%E6%98%AF%E5%9B%A2%E9%98%9F%E3%80%82"));

//            System.out.println(CoderUtil.decoder("%E9%A5%AE%E9%A3%9F"));

//            System.out.println("-" + CoderUtil.encoder("") + "-");

//            String str = "\u0001";

//            System.out.println(str);


//            String str = "123456789";
//            System.out.println(str.length());
//            str = str.substring(0, str.length() - 1);
//            System.out.println(str);

//            StringBuffer buf = new StringBuffer();
//            buf.append("a");
//            buf.append("b");
//            buf.append("c");
//            buf.append("d");
//            buf.append("e");
//
//            System.out.println(buf.toString());
//
//            buf.delete(0, buf.length());
//
//            System.out.println("-" + buf.toString() + "-");

//            String path = "hdfs://linux01:8020/binlog_output/UPDATE/product_hive/cash_record/2018-08-22/10/20";
//            String[] paths = path.split("cash_record");

//            System.out.println(paths[0]);
//            System.out.println(paths[1]);

//            String[] dateArray = paths[1].split("/");
//            System.out.println(dateArray[1] + " " + dateArray[2] + ":" + dateArray[3] + ":00");

            /*
            String partitionTime = "2018-08-22 10:20:00";

            Date date = DateUtils.parseTime(partitionTime);

            String year = String.format(Constants.YEAR, date);
            String month = String.format(Constants.MONTH, date);
            String day = String.format(Constants.DAY, date);

            String hour = String.format(Constants.HOUR, date);
            String min = String.format(Constants.MIN, date);
            String min10 = DateUtils.getMIN10(min); // 取整10分钟

            System.out.println(year);
            System.out.println(month);
            System.out.println(day);
            System.out.println(hour);
            System.out.println(min);
            System.out.println(min10);
            */

//        	String str = "product_hive.cash_record";
//        	String[] strs = str.split("\\.");
//        	System.out.println(strs.length);
        	
        	System.out.println(StringUtils.isNotBlank("     1      "));
        	
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
