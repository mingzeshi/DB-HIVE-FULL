package com.jy.test;

import net.sf.json.JSONObject;

import com.jy.bean.loadHiveBean.hdfsLoadHiveDWBean.HiveDWDataSet;

public class JsonToBean {
	
	public static void main(String[] args) {
		HiveDWDataSet dataSet1 = null;
		//new HiveKZDataSet();
		System.out.println(dataSet1 == null);
		
		String dataSet1JSON = JSONObject.fromObject(dataSet1).toString();
		System.out.println(dataSet1JSON == null);
		
		HiveDWDataSet dataSet2 = (HiveDWDataSet) JSONObject.toBean(JSONObject.fromObject(dataSet1JSON), HiveDWDataSet.class);
		System.out.println(dataSet2 == null);
	}

}
