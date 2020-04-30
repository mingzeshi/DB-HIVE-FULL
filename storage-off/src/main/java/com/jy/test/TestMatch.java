package com.jy.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestMatch {
	
	public static void main(String[] args) {
		// 按指定模式在字符串查找
	      String line = "C95615F0BF637F860C2BD032CEF9AD8F,cff979f17659473cbfaa7045c6eed117,C92E6575-5082-48F6-AE92-58FBB0B3AB56,252EFFB5-C8FF-4DF5-8DB9-DBA1BF2671971562757865125,https://m.jianlc.com//app/notice.html?id=D0B717DB30CD4E3AA55D7677040CB0DA##{\"shareImageUrl\":\"\",\"shareTitle\":\"\",\"shareLinkUrl\":\"?id=D0B717DB30CD4E3AA55D7677040CB0DA\",\"wechatShareLinkUrl\":\"?id=D0B717DB30CD4E3AA55D7677040CB0DA\",\"shareDescription\":\"\",\"shareShowType\":1,\"shareType\":2},3,1562757865126,,1573181714863,10.11.21.26,{\"shareDescription\":\"\",\"shareShowType\":1,\"shareType\":2}";

//	      String pattern = "\\{.*(?=\\})\\}";
	      String pattern = "(\\{[\\s\\S]*?\\})";
	 
	      
	      Pattern r = Pattern.compile(pattern); // 创建 Pattern 对象
	 
	      Matcher m = r.matcher(line); // 现在创建 matcher 对象
	      
	      while (m.find()) {
	    	  String old_str = m.group(1);
	    	  
	    	  String new_str = old_str.replace(",", "，");
	    	  
	    	  line = line.replace(old_str, new_str);
	      }
	}
}
