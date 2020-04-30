package com.jy.test;

import com.jy.util.HadoopTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsUtils;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestHDFS {
    public static void main(String[] args) {

//        String path = "/mrdata/input/word";

        try {
//            HDFSClient.readLine(path);

//            FileStatus[] fileStatucArray = HDFSClient.getGlobStatus("/aa/*");

//            for(FileStatus fileStatus : fileStatucArray) {
//                String hdfsPath = fileStatus.getPath().getName();
//                System.out.println(hdfsPath);
//            }

            // /aa/bb/cc/dd/ee/ff/

            FileSystem fs = FileSystem.get(new URI("hdfs://linux01:8020"), new Configuration(), "hadoop");

            /*
            Set<String> pathList = new HashSet<String>();

            HadoopTool.containFileDir(fs, new Path("/binlog_output/UPDATE"), pathList);

            for(String path : pathList) {
                System.out.println(path);
            }
            */

            /*
            List<String> textList = HadoopTool.readTextFromFile(fs, new Path("/jlcstore/datamerge_schema/schema.json"));

            for(String line : textList) {
                System.out.println(line);
            }
            */
            
            boolean exists = HadoopTool.fileExists(fs, new Path("/jlc/distill_data/20180830111922/UPDATE/product_hive/bank_card/part_log_day=2018-08-30/hour=00/min=33"));
            System.out.println(exists);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

//    public static FileStatus[] getGlobStatus(String inputPathItem) throws IOException {
//        FileStatus[] newList = fs.globStatus(new Path(inputPathItem));
//        return newList;
//    }

}
