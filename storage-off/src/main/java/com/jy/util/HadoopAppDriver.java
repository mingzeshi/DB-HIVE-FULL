package com.jy.util;

import org.apache.hadoop.util.ProgramDriver;

import com.jy.compress.CompressFile;
import com.jy.compress.CompressMR;
import com.jy.mr.HdfsToHive;
import com.jy.mr.ImportDBLoadToHive;
import com.jy.mr.ImportDBTable;
import com.jy.mr.ImportDBToHive;

public class HadoopAppDriver {

    public static int exec(String[] args) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();
        try {
//            programDriver.addClass("binlog.partition", BinlogPartitionMR.class, "binlog数据分区");
//            programDriver.addClass("data.merge", DataMergeMR.class, "数据合并");
            programDriver.addClass("db.data.import", ImportDBTable.class, "DB数据导入到HDFS");
            programDriver.addClass("hdfs.load.hive", HdfsToHive.class, "HDFS数据加载到HIVE");
            programDriver.addClass("import.db.load.hive", ImportDBLoadToHive.class, "DB数据加载到HIVE");
            programDriver.addClass("db.import.hive", ImportDBToHive.class, "DB数据导入到HIVE");
            programDriver.addClass("hdfs.io.compress", CompressFile.class, "HDFS数据压缩与解压缩");
            programDriver.addClass("hdfs.io.compress.mr", CompressMR.class, "HDFS数据压缩-MR");
            
            programDriver.driver(args);
            exitCode = 0;
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return exitCode;
    }

    public static void main(String[] args) {
    	
//    	args = new String[] {
////    			"db.data.import",
////    			"-DconfigFile=Common.xml",
////    			"D:/mr_file/20190211/output_wind/"
//    			
////    			"hdfs.load.hive",
////    			"-DconfigFile=Common.xml",
////    			"D:/mr_file/20190211/output/2019-02-26/product/*/*"
//    			
////    			"import.db.load.hive",
////    			"-DconfigFile=Common.xml",
////    			"D:/mr_file/20190211/output001/"
//    			
////    			"import.db.load.hive_b",
////    			"-DconfigFile=Common.xml",
////    			"D:/mr_file/20190211/output001/"
//    			
//    			"db.import.hive",
//    			"-DconfigFile=Common.xml",
    			
//    			"hdfs.io.compress.mr",
//    			"-DconfigFile=Common.xml",
//    			"D:/mr_file/20200224/input/*/",
//    			"D:/mr_file/20200224/ouput/"
//    	};
    	
        int exitCode = exec(args);
        System.exit(exitCode);
    }
}
