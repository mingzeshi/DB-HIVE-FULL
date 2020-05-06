package org.frozen.test;

import java.util.HashMap;
import java.util.Map;

public class TestHashMap {

	public static void main(String[] args) {
		Map<String, String> testMap = new HashMap<String, String>();
		testMap.put("AA", "ha");
		testMap.put("BB", "hb");
		testMap.put("cc", "hb");
		
		String key = "AA";
		System.out.println(testMap.containsKey(key));

	}
	
}
