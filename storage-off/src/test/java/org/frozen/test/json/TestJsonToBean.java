package org.frozen.test.json;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.frozen.bean.loadHiveBean.HiveDataSet;

import net.sf.json.JSONObject;

public class TestJsonToBean {

	public static void main(String[] args) {
		Map<String, HiveDataSet> hiveDataSetMap1 = new HashMap<String, HiveDataSet>();
		HiveDataSet hiveDataSet = new HiveDataSet();
		hiveDataSet.setEnnameM("user_info");
		hiveDataSet.setEnnameH("user_info_h");
		
		hiveDataSetMap1.put("a", hiveDataSet);
		hiveDataSetMap1.put("b", hiveDataSet);
		hiveDataSetMap1.put("c", hiveDataSet);
		
		String input = JSONObject.fromObject(hiveDataSetMap1).toString();
		
		System.out.println(input);
		
		// ----------------------------------------------------
		
		JSONObject jsonObject = JSONObject.fromObject(input.toString());
		
		Iterator<String> jsonKeys = jsonObject.keys();
		
		Map<String, HiveDataSet> hiveDataSetMap2 = new HashMap<String, HiveDataSet>();
		
		while(jsonKeys.hasNext()) {
			String jK = jsonKeys.next();
			hiveDataSetMap2.put(
					jK,
					(HiveDataSet) JSONObject.toBean(JSONObject.fromObject(jsonObject.get(jK)), HiveDataSet.class));
		}
		
		System.out.println(JSONObject.fromObject(hiveDataSetMap2).toString());
	}
	
}
