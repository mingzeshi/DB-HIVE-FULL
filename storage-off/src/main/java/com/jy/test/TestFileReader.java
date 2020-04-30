package com.jy.test;

import java.io.BufferedReader;
import java.io.FileReader;

public class TestFileReader {

	public static void main(String[] args) {
		try {
			String thisLine = null;
			
			BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\文件\\20191129\\check_db_tables_2019-11-29-15-25"));

	         while ((thisLine = br.readLine()) != null) {
	            System.out.println(thisLine);
	         }       
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
