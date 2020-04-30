package com.jy.test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateTest {

    public static void main(String[] args) {

        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            Date date = format.parse("2016-10-13 15:05:46");

            String year = String.format("%tY", date);
            String month = String.format("%tm", date);
            String day = String.format("%td", date);

            String hour = String.format("%tH", date);
            String min = String.format("%tM", date);

            System.out.println(year);
            System.out.println(month);
            System.out.println(day);

            System.out.println(hour);
            System.out.println(min);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
