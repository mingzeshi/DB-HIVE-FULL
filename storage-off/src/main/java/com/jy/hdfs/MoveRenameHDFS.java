package com.jy.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.jy.util.HadoopUtil;

public class MoveRenameHDFS {

	public static void main(String[] args) {
		
		String targetPath = args[0];
		
//		FileSystem fileSystem = FileSystem.get(new Configuration());
		
//		HadoopUtil.mvAlsoMerge(fileSystem, targetPath, pathSet);
	}

}
