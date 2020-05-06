package org.frozen.test;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;

public class HdfsBlockLocation {
	
	public static void main(String[] args) {
		try {
			FileSystem fs = FileSystem.get(new URI("hdfs://10.103.27.165:8020"), new Configuration(), "root");
//			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] listStatus = fs.listStatus(new Path("/files/20190212_002_1"));
			
			for(FileStatus file : listStatus) {
				long length = file.getLen();

				BlockLocation[] blkLocations;
		        if (file instanceof LocatedFileStatus) {
		          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
		        } else {
		          blkLocations = fs.getFileBlockLocations(file, 0, length);
		        }
		        
		        for(BlockLocation block : blkLocations) {
		        	String[] cachedHosts = block.getCachedHosts();
		        	System.out.println(cachedHosts.length);

		        	for(String hosts : cachedHosts) {
		        		System.out.println(hosts);
		        	}
		        	
		        }
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
