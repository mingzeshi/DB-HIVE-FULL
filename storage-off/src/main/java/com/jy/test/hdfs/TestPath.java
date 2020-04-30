package com.jy.test.hdfs;

import java.util.Stack;

import org.apache.hadoop.fs.Path;

public class TestPath {
	
	public static void main(String[] args) {
		Path path = new Path("C:\\Users\\Administrator\\Desktop\\source\\java\\08_JDK源码剖析系列\\02_Java并发编程系列\\031~052资料\\44_wait与notify的底层原理：monitor以及wait set");
		
		Path parentPath = path;
		
		Stack<String> pathStack = new Stack<String>();
		
		while(parentPath != null) {
			
			pathStack.add(parentPath.getName());
			
			parentPath = parentPath.getParent();
			
		}
		
		StringBuffer buf = new StringBuffer();
		while(!pathStack.isEmpty()) {
			buf.append(pathStack.pop() + "/");
		}
		
		System.out.println(buf.toString());
	}

}
