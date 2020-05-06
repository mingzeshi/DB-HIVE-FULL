package org.frozen.test.hdfs;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.frozen.util.HadoopUtil;

public class RenameHDFSFile {
	
	public static void main(String[] args) {
		
		System.out.println("--------------------START--------------------");
		
//		args = new String[] {
//				"hdfs://slavenode165.data.test.ds:8020/",
//				"/files/20200201/part_log_day=2020-01-01"
//		};
		
		try {
			FileSystem fs = FileSystem.get(new URI(args[0]), new Configuration(), "root");			
			
			Set<String> pathList = new HashSet<String>();
			
			HadoopUtil.containFilePath(fs, new Path(args[1]), pathList);
			
			for(String filePath : pathList) {
				if(filePath.contains(".inprogress.")) {
					
					
					String fileNewName = new Path(filePath).getParent().toString() + "/" + UUID.randomUUID().toString();
					
					HadoopUtil.rename(fs, new Path(filePath), new Path(fileNewName));
					
//					System.out.println("old name : " + filePath);
//					System.out.println("new name : " + fileNewName);
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("--------------------END--------------------");
	}

}
