package org.frozen.test;

import org.frozen.constant.Constants;

public class TestStrTrim {

	public static void main(String[] args) {
		String name = "邢亚飞\n";
		System.out.println(name.trim() + "hahaha");
		
		System.out.println(name.contains(Constants.LINE_N));
		System.out.println(name);
		System.out.println(name.replaceAll(Constants.LINE_N, ""));
	}
	
}
