package org.frozen.test;

public class TestStr {
	
	public static void main(String[] args) {
		
//		String conditions = "create_time > date_sub(curdate(),interval 1 day) or update_time > date_sub(curdate(),interval 1 day)";
		String conditions = "created > date_sub(curdate(),interval 1 day) or modified > date_sub(curdate(),interval 1 day)";
		
//		String params = ExportConditions.parserSQL(conditions);
//		System.out.println(params);
	}

}
