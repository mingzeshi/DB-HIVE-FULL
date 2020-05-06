package org.frozen.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetTablesAndColumns {
	/**
	 * 获取脚本中的所有脚本片段
	 * @param inStr
	 * @return
	 */
	public List<String> getAllScripts(String inStr){
		System.out.println("===================\n脚本开始截取\n===================");
		List<String> scripts = new ArrayList<String>();
		Stack<Integer> rightBracketsStatck = new Stack<Integer>();
 
		//逆序匹配左右括号截取脚本
		while(inStr.length() > 0){
			boolean foundScript = false;
			char[] scriptChar = inStr.toCharArray();
			int rightBracketsIndex = -1;
			int leftBracketsIndex = -1;
			
			for(int i = scriptChar.length-1; i >= 0; i--){
				if((int)scriptChar[i] == 41){ //右括号
					rightBracketsStatck.push(i);
				}
				if((int)scriptChar[i] == 40){ //左括号
					rightBracketsIndex = rightBracketsStatck.pop();
					leftBracketsIndex = i;
					
					String judgeStr = inStr.substring(i);
					if(judgeStr.length() > 8){
						judgeStr = inStr.substring(i, i+8);
						if("(select ".equals(judgeStr)){
							String getScript = inStr.substring(i, rightBracketsIndex+1);
							scripts.add(getScript);
							System.out.println("获得脚本段："+getScript);
							foundScript = true;
							break;
						} else{
							//没找到，标识
							foundScript = false;
						}
					} else {
						//剩余字符已不足
						foundScript = false;
					}
				}
			}
			
			if(foundScript){
				inStr = inStr.substring(0, leftBracketsIndex) + inStr.substring(rightBracketsIndex+1);
				rightBracketsStatck.clear();
			} else {
				System.out.println("最后剩余脚本："+inStr);
				scripts.add(inStr);
				break;
			}
			
		}
		System.out.println("===================\n脚本截取完毕\n===================\n");
		return scripts;
	}
	
	/**
	 * 获取语句中的表名及别名
	 * @param inStr
	 * @return
	 * @throws Exception
	 */
	public Map<String, String> getTablesAndAlias(String inStr, Map<String, String> tablesAndAliasMap) throws Exception{
		System.out.println("===================\n开始获取表名及别名\n===================");
		System.out.println("当前脚本："+inStr);
		//表名
		String tableName = "";
		//别名
		String aliasName = "";
		/**
		 * from关键字开始的字符串
		 * 有join查询，就截取到join关键字
		 * 没有join查询，就截取到where关键字
		 */
		String fromStr = "";
		
		
		int fromStart = inStr.indexOf("from");
		int whereStart = inStr.indexOf("where");
		if(fromStart < 0){
			throw new Exception("脚本中没有from关键字");
		} else {
			//如果没有where关键字，默认没有使用条件查询
			if(whereStart == -1){
				whereStart = inStr.length();
			}
		}
		//获取from到where间的字符串
		String from2WhereStr = inStr.substring(fromStart+5, whereStart).trim();
		
		
		int firstJoinStart = -1;
		//利用正则匹配三种join查询的关键字
	    String joinToFind = "left join|right join|inner join";
	    Pattern pattern = Pattern.compile(joinToFind);
	    Matcher match = pattern.matcher(from2WhereStr);
	    if (match.find()) {
	    	//匹配到join查询
	    	firstJoinStart = match.start();
	    }
		
		if(firstJoinStart > 0){
			fromStr = from2WhereStr.substring(0, firstJoinStart);
			//取join后的表名及别名
			this.getJoinPartTablesAndAlias(from2WhereStr, tablesAndAliasMap);
		} else {
			fromStr = from2WhereStr;
		}
		
		//取from后的表名及别名
		String[] couples = fromStr.split(",");
		if(couples.length > 0){
			for(int i = 0, length = couples.length; i < length; i++){
				String tableAndAlias = couples[i].trim();
				tableAndAlias.replaceAll(" +", " ");
				String[] splitStr = tableAndAlias.split(" ");
				if(splitStr.length > 1){
					tableName = splitStr[0];
					aliasName = splitStr[1];
				} else {
					throw new Exception("脚本中没有设置表的别名");
				}
				System.out.println("找到表："+tableName+"，对应别名："+aliasName);
				tablesAndAliasMap.put(aliasName, tableName);
			}
		}
		
		System.out.println("===================\n获取表名及别名结束\n===================\n");
		return tablesAndAliasMap;
	}
	
	/**
	 * 获取关联查询部分表名及别名
	 * @param inStr
	 * @param tablesAndAliasMap
	 * @throws Exception
	 */
	public void getJoinPartTablesAndAlias(String inStr, Map<String, String> tablesAndAliasMap) throws Exception{
		String joinToFind = "left join|right join|inner join";
		Pattern pattern = Pattern.compile(joinToFind);
	    
		int joinStart = -1;
		int joinEnd = -1;
		int onStart = -1;
		int onEnd = -1;
		String tableName = "";
		String aliasName = "";
		
		while(inStr.length() > 0){
			Matcher match = pattern.matcher(inStr);
		    if (match.find()) {
		    	joinStart = match.start();
		    	joinEnd = match.end();
		    	
		    	onStart = inStr.indexOf(" on ")+1;
		    	if(onStart == 0){
		    		throw new Exception("脚本有误，关联查询缺少条件");
		    	}
		    	onEnd = onStart + 2;
		    	
		    	//取join关键字和on关键字之间的字符串
		    	String tableAndAlias = inStr.substring(joinEnd, onStart).trim();
		    	String[] splitStr = tableAndAlias.split(" ");
				if(splitStr.length > 1){
					tableName = splitStr[0].trim();
					aliasName = splitStr[1].trim();
				} else {
					throw new Exception("脚本中没有设置表的别名");
				}
				System.out.println("找到表："+tableName+"，对应别名："+aliasName);
				tablesAndAliasMap.put(aliasName, tableName);
		    	
				//取完当前join关键字后的表、别名后，截断当前字符串
		    	inStr = inStr.substring(onEnd);
		    } else {
		    	//没有join关键字，字符串置空，跳出循环
		    	inStr = "";
		    }
		}
	}
	
	/**
	 * 获取脚本中使用到的表字段
	 * @param inStr
	 * @return
	 */
	public Set<String> getParams(String inStr){
		System.out.println("===================\n开始获取表中的参数\n===================");
		System.out.println("当前脚本："+inStr);
		
		Set<String> params = new HashSet<String>();
		
		//匹配表名/别名.字段（表名/别名、字段都只包含小写英文、数字、下划线）
		//或匹配表名/别名.*
		String paramsToFind = "[a-z0-9_]+\\.[a-z0-9_]+|[a-z0-9_]+\\.\\*";
	    Pattern pattern = Pattern.compile(paramsToFind);
	    Matcher match = pattern.matcher(inStr);
	    while (match.find()) {
	    	String param = inStr.substring(match.start(), match.end());
	    	System.out.println("找到参数：" + param);
	    	params.add(param);
	    }
	    
	    System.out.println("===================\n获取表中的参数结束\n===================\n");
		return params;
	}
	
	/**
	 * 获取脚本中使用的表及字段
	 * @param scriptFile
	 * @return
	 */
	public Map<String, Set<String>> getTablesAndUsedColumns(File scriptFile){
		Map<String, Set<String>> tablesAndUsedColumns = new HashMap<String, Set<String>>();
		Map<String, String> tablesAndAliasMap = new HashMap<String, String>();
		
		try {
			InputStreamReader is = new InputStreamReader(new FileInputStream(scriptFile),"UTF-8");
	        BufferedReader br = new BufferedReader(is);
			String line = "";
			while ((line = br.readLine()) != null) {
				line = line.toLowerCase().trim();
				System.out.println("\n=====================================================读取一行脚本=====================================================\n");
				System.out.println("源脚本: "+line+"\n");
				
				List<String> toJudgeScript = this.getAllScripts(line);
				
				//先获取当前脚本中所有的表及别名
				for(String script : toJudgeScript){
					try {
						tablesAndAliasMap = this.getTablesAndAlias(script, tablesAndAliasMap);
					} catch (Exception e) {
						System.out.println("获取别名出错："+e.getMessage()+" 问题脚本：\n" + script);
					}
				}
				
				//再次遍历获取参数并输出
				for(String script : toJudgeScript){
					try {
						Set<String> params = this.getParams(script);
						
						Iterator<String> i = params.iterator();
						
						if(!i.hasNext()){
							throw new Exception("脚本无参数");
						}
						
						while(i.hasNext()){
							String aliasParam = i.next();
							String[] splitStr = aliasParam.split("\\.");
							if(splitStr.length > 1){
								String alias = splitStr[0].trim();
								String param = splitStr[1].trim();
								String tableName = tablesAndAliasMap.get(alias);
								Set<String> usedColumns = tablesAndUsedColumns.get(tableName);
								if(usedColumns == null){
									usedColumns = new HashSet<String>();
									tablesAndUsedColumns.put(tableName, usedColumns);
								}
								usedColumns.add(param);
							} else {
								throw new Exception("参数"+aliasParam+"错误");
							}
						}
					} catch (Exception e) {
						System.out.println("出错："+e.getMessage()+"，问题脚本：\n" + script);
					}
				}
				tablesAndAliasMap.clear();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("遇到了意外！" + e.getMessage());
		}
		
		System.out.println("获取完毕：");
		//对表名排个序
		List<String> tablesList = new ArrayList<String>(tablesAndUsedColumns.keySet());
		Collections.sort(tablesList);
		
		for(String tableName : tablesList){
			System.out.println(tableName + "表：" + tablesAndUsedColumns.get(tableName));
		}
		
		return tablesAndUsedColumns;
	}
	
	public static void main(String[] args) {
		GetTablesAndColumns gtac = new GetTablesAndColumns();
//        String fileName = "F:\\tmp\\scriptText.txt";
//		File scriptFile=new File(fileName);
//		gtac.getTablesAndUsedColumns(scriptFile);
		
		List<String> param = gtac.getAllScripts("select * from a where create_time > date_sub(curdate(),interval 1 day) or update_time > date_sub(curdate(),interval 1 day)");
		System.out.println(param);
	}

}
