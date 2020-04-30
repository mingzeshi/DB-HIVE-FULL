package com.jy.test;

import java.util.HashSet;
import java.util.Set;

public class TestHashSet {

	public static void main(String[] args) {
		
		Set<String> set1 = new HashSet<String>();
		set1.add("a");
		set1.add("b");
		set1.add("c");
		set1.add("d");

		Set<String> set2 = new HashSet<String>();
		set2.add("d");
		set2.add("e");
		set2.add("b");
		set2.add("g");
		
		for(String key : set2) {
			if(set1.contains(key)) {
				set1.remove(key);
			}
		}
		
		for(String k : set1) {
			System.out.println(k);
		}
	}
	
}
