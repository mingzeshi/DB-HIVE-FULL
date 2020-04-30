package com.jy.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;


public abstract class HadoopTool extends Configured implements Tool {

    private static final Logger log = LoggerFactory.getLogger(HadoopTool.class);

    public static final String JOB_NAME = MRJobConfig.JOB_NAME;
    public static final Charset UTF8 = Charset.forName("UTF-8");

    public static void execMain(HadoopTool tool, String[] args) throws Exception {

        String toolId = tool.getClass().getSimpleName();

        try {
            log.info("exec start: ---------------");

            Configuration configuration = new Configuration();
            
            GenericOptionsParser parser = new GenericOptionsParser(configuration, args);
            String[] toolArgs = parser.getRemainingArgs();

            String configFilePath = configuration.get("configFile");
            String tempConfig = configuration.get("tempConfig");
            
            configuration.addResource("Common.xml");
            
            if(StringUtils.isNotBlank(tempConfig)) { // 用于测试
            	configuration.addResource(new Path(tempConfig));
            }

            if(StringUtils.isNotBlank(configFilePath)) {
            	configuration.addResource(new Path(configFilePath));
            } else {
            	log.info("找不到：configFile参数，例：-DconfigFile=/config/configfile.xml");
            	return;
            }

            String val;
            if (configuration.get(JOB_NAME) == null && configuration.get("job.name.db") == null) {
                val = toolId + "_" + DateUtils.formatNumber(new Date());
                configuration.set(JOB_NAME, val);
            } else if(configuration.get("job.name.db") != null) {
            	val = toolId + "_" + DateUtils.formatNumber(new Date()) + "_" + configuration.get("job.name.db");
       			configuration.set(JOB_NAME, val);
            }

            tool.setConf(configuration);

            tool.run(toolArgs);
        } catch (Exception e) {
            log.info("exec failed: " + toolId, e);
        } finally {
            log.info("exec end: ---------------");
        }

    }

    public static Job getJobInstance(Configuration configuration) throws IOException {
        return Job.getInstance(configuration);
    }

    public static String getJobState(Job job) throws InterruptedException, IOException {
        return job.getStatus().getState().toString().toString();
    }

    /**
     * 按行读取文件内容.
     *
     * @param path
     * @return
     * @throws IOException
     */
    public static List<String> readTextFromFile(FileSystem fs, Path path) throws IOException {
        if (!fs.exists(path)) {
            return Lists.newArrayList();
        }
        if (fs.isDirectory(path)) {
            throw new IOException("Path is not File: " + path);
        }
        InputStream input = fs.open(path);
        List<String> lines = IOUtils.readLines(input, UTF8);
        IOUtils.closeQuietly(input);
        return lines;
    }


    /**
     * 返回包含文件的所有目录
     * @param fs
     * @param path
     * @param pathList
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static Set<String> containFileDir(FileSystem fs, Path path, Set<String> pathList) throws FileNotFoundException, IOException{

        FileStatus[] fileStatuses = fs.listStatus(path);

        for(FileStatus fileStatus : fileStatuses) {

            if (fileStatus.isDirectory()) {
                String dirPath = fileStatus.getPath().toString();
                containFileDir(fs, new Path(dirPath), pathList);
            } else {
                Path parentPath = fileStatus.getPath().getParent();
                pathList.add(parentPath.toString());
            }
        }
        return pathList;
    }
    
    /**
     * 判断文件是否存在
     * @param fs
     * @param path
     * @return
     * @throws IOException 
     */
    public static boolean fileExists(FileSystem fs, Path path) throws IOException {
    	return fs.exists(path);
    }
}
