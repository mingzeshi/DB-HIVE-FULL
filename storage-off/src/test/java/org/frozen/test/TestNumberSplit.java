package org.frozen.test;

import java.math.BigDecimal;

import org.frozen.constant.Constants;

public class TestNumberSplit {

	public static void main(String[] args) {
		int dataCount = 14890483;
		int batchCount = 3000000;
		
		Integer numSplits = 1;
		if(dataCount > (batchCount * 1.2)) {
			BigDecimal bigDecimal = new BigDecimal(dataCount).divide(new BigDecimal(batchCount), 2);			
			String[] nums = String.valueOf(bigDecimal.doubleValue()).split(Constants.SPOT);
			Long num = Long.parseLong(nums[1]);
			if(num > 0) {
				numSplits = Integer.parseInt(nums[0]) + 1;
			} else {
				numSplits = Integer.parseInt(nums[0]);
			}
		}
		
		System.out.println(numSplits);
	}
}
