package org.frozen.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.frozen.util.HadoopUtil;

public class MoveRenameHDFS {

	public static void main(String[] args) {
		
		String targetPath = args[0];
		
//		FileSystem fileSystem = FileSystem.get(new Configuration());
		
//		HadoopUtil.mvAlsoMerge(fileSystem, targetPath, pathSet);
	}

}
