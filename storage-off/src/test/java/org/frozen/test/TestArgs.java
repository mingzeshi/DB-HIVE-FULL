package org.frozen.test;

public class TestArgs {
	
	public static void main(String[] args) {
		String a = System.getProperty("a", "A");
		String b = System.getProperty("b", "B");
		String c = System.getProperty("c", "C");
		
		System.out.println("======" + a);
		System.out.println("======" + b);
		System.out.println("======" + c);
	}

}
