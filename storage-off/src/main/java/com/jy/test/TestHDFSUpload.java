package com.jy.test;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class TestHDFSUpload {
	
	public static void main(String[] args) {
		
		try {
			Configuration configuration = new Configuration();
			configuration.set("fs.defaultFS", "hdfs://10.103.27.164:8020");
			
//			FileSystem fs = FileSystem.get(configuration);
			FileSystem fs = FileSystem.get(new URI("hdfs://10.103.27.164:8020"), configuration, "hdfs");

			Path srcPath = new Path("C:/Users/Administrator/Desktop/文件/20190211/upload/20190212_002"); //本地上传文件路径
			Path dstPath = new Path("/files/"); //hdfs目标路径
			System.out.println("------uploading------");
			fs.copyFromLocalFile(true, srcPath, dstPath);
			System.out.println("------success------");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
