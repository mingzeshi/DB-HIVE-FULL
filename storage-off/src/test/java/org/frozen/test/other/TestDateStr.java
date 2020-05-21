package org.frozen.test.other;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TestDateStr {
	
	public static void main(String[] args) {
		String dateStr = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
			
		System.out.println(dateStr);


	}

}
