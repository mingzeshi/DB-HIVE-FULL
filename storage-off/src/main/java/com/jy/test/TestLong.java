package com.jy.test;

import java.math.BigDecimal;

public class TestLong {

	public static void main(String[] args) {
		
		long batchCount = 500000;
		long count = 7275880;

		long chunks = 1;
		
		if(count > (batchCount * 1.2)) {
			BigDecimal bigDecimal = new BigDecimal(count).divide(new BigDecimal(batchCount));
			System.out.println("------" + bigDecimal.doubleValue());
			String[] nums = String.valueOf(bigDecimal.doubleValue()).split("\\.");
			Long num = Long.parseLong(nums[1]);
			if(num > 0) {
				chunks = Long.parseLong(nums[0]) + 1;
			}
		}
		long chunkSize = (count / chunks);
		
		System.out.println(chunks);
		System.out.println(chunkSize);
	}
	
}
