package org.frozen.test;

import java.util.HashSet;
import java.util.Set;

public class TestSetRemoveAll {
	
	public static void main(String[] args) {
		Set<String> setA = new HashSet<String>();
		setA.add("A1");
		setA.add("A2");
		setA.add("A3");
		setA.add("A4");
		setA.add("A5");
		
		Set<String> setB = new HashSet<String>();
		setB.add("A1");
		setB.add("A2");
		setB.add("A3");
//		setB.add("A4");
//		setB.add("A5");
		setB.add("B1");
		setB.add("B2");
		setB.add("B3");
		
		setA.removeAll(setB);
		System.out.println(setA);
	}

}
