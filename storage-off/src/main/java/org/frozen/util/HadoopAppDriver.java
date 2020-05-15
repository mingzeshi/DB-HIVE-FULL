package org.frozen.util;

import org.apache.hadoop.util.ProgramDriver;
import org.frozen.compress.CompressFile;
import org.frozen.compress.CompressMR;
import org.frozen.monitoring.TableBalance;
import org.frozen.mr.ImportDBToHive;

public class HadoopAppDriver {

    public static int exec(String[] args) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();
        try {
            programDriver.addClass("db.import.hive", ImportDBToHive.class, "DB数据导入到HIVE-MR");
            programDriver.addClass("hdfs.io.compress", CompressFile.class, "HDFS数据压缩与解压缩");
            programDriver.addClass("hdfs.io.compress.mr", CompressMR.class, "HDFS数据压缩-MR");
            programDriver.addClass("hive.table.banlace", TableBalance.class, "检查Hive表与数据库表、字段是否平衡");
            
            programDriver.driver(args);
            exitCode = 0;
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return exitCode;
    }

    public static void main(String[] args) {
        int exitCode = exec(args);
        System.exit(exitCode);
    }
}
