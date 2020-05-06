package org.frozen.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.frozen.bean.DataDistinctBean;
import org.frozen.bean.SchemaBean;
import org.frozen.comparator.DataDistinctBeanComparator;
import org.frozen.constant.Constants;
import org.frozen.util.DateUtils;
import org.frozen.util.HadoopTool;

public class DataMergeMR extends HadoopTool {
	
    public static void main(String[] args) throws Exception {
        execMain(new DataMergeMR(), args);
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        
        FileSystem fileSystem = FileSystem.get(configuration);
//        FileSystem fileSystem = FileSystem.get(new URI("hdfs://linux01:8020"), configuration, "hadoop");

        Job job  = getJobInstance(configuration);

        job.setJarByClass(DataMergeMR.class);

//        job.setCombinerClass(); // map端输出聚合

        job.setReducerClass(MergeReducer.class);

        job.setMapOutputKeyClass(DataDistinctBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(DataDistinctBeanComparator.class);
        
        job.setNumReduceTasks(configuration.getInt("mapreduce.job.reduces", 20)); // reduce task 数量，默认20

        //设置输入目录
        // 加载新数据
        String distillPath = configuration.get("distill.dir", "/jlc/distill_data/") + args[0];

        Set<String> pathList = new HashSet<String>();

        containFileDir(fileSystem, new Path(distillPath + "/" + Constants.MERGE_DATA), pathList); // 获取将要合并新数据的所有目录

        for(String path : pathList) {
            MultipleInputs.addInputPath(job, new Path(path), CombineTextInputFormat.class, LoadDistillMapper.class);
        }

        // 加载历史数据
        String schemaPath = configuration.get("schema.path", "/jlcstore/datamerge_schema/schema.json");

        List<SchemaBean> schemaList = getSchema(fileSystem, schemaPath.toString());

        for(SchemaBean schemaBean : schemaList) {
            String hive_db = schemaBean.getHive_db();
//            String table = schemaBean.getTable();

            for(String path : pathList) {
                if(path.contains(hive_db)) { // 如果新数据路径中包含hivedatabase，说明该新数据与此hivedatabase中数据合并
                    String dataRelativePath = path.split(hive_db)[1]; // hive 数据相对路径 例如： ....../bank_card/2018-08-20/12/30
                    String dataAbsolutePath = schemaBean.getHive_db_path() + "/" + dataRelativePath; // hive 数据完整路径

                    Path historyPath = new Path(dataAbsolutePath);
                    if(fileExists(fileSystem, historyPath)) {
                    	MultipleInputs.addInputPath(job, historyPath, CombineTextInputFormat.class, LoadHistoryMapper.class); // 加载历史数据
                    }

//                    String db_table = configuration.get("jlc.db.table");
//                    configuration.set("jlc.db.table", db_table + Constants.DATAKV + hive_db + "." + table);
                }
            }
        }
        
        // 设置切片大小
        CombineTextInputFormat.setMaxInputSplitSize(job, Constants.SPLIT_MAX);
        CombineTextInputFormat.setMinInputSplitSize(job, Constants.SPLIT_MIN);

        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 设置输出目录

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 从HDFS读取表更新与分区schema
     * @param path
     * @return
     * @throws Exception
     */
    public static List<SchemaBean> getSchema(FileSystem fileSystem, String path) throws Exception {
        List<SchemaBean> schemaBeanList = new ArrayList<SchemaBean>();

        List<String> lineList = readTextFromFile(fileSystem, new Path(path));

        for(String line : lineList) {
            SchemaBean schemaBean = (SchemaBean) JSONObject.toBean(JSONObject.fromObject(line), SchemaBean.class);
            schemaBeanList.add(schemaBean);
        }
        return schemaBeanList;
    }

    /**
     * 处理新数据mapper
     */
    public static class LoadDistillMapper extends Mapper<LongWritable, Text, DataDistinctBean, Text> {

        DataDistinctBean outputKey = new DataDistinctBean();
        Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] datas = line.split(Constants.TAB);

            String mergeKey = datas[0]; // product_hive.cash_record-=10699793-=2018-08-29 16:23:32-=1535212816000
            String dataValue = datas[1]; // data

            String[] fields = mergeKey.split(Constants.DATAKV);
            
            if(fields.length > 3) {
	            String db_table = fields[0];
	            String unique = fields[1];
	            String partition_time = fields[2];
	            Long exec_tiime = Long.parseLong(fields[3]);
	
	            String[] dbortable = db_table.split("\\.");
	            if(dbortable.length > 1) {
	            	String hive_db = dbortable[0];
	            	String table = dbortable[1];
	            
	            	outputKey.set(hive_db, table, unique, partition_time, exec_tiime);
	            } else {
	            	outputKey.set("aaa", "bbb", UUID.randomUUID().toString(), "2018-08-31 00:00:00", 0L);
	            }
            } else {
            	outputKey.set("aaa", "bbb", UUID.randomUUID().toString(), "2018-08-31 00:00:00", 0L);
            }
            
            outputValue.set(dataValue);
            context.write(outputKey, outputValue);

        }
    }

    /**
     * 处理历史数据mapper
     */
    public static class LoadHistoryMapper extends  Mapper<LongWritable, Text, DataDistinctBean, Text> {
        DataDistinctBean outputKey = new DataDistinctBean();
        Text outputValue = new Text();

        FileSystem fileSystem;
        List<SchemaBean> schemaList;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                Configuration configuration = context.getConfiguration();
//configuration.set("fs.defaultFS", "hdfs://linux01:8020");
                
                fileSystem = FileSystem.get(configuration);

                String schemaPath = configuration.get("schema.path", "/jlcstore/datamerge_schema/schema.json");

                schemaList = getSchema(fileSystem, schemaPath.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        			
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {            
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileLocation = fileSplit.getPath().toString();
            String unique = value.toString().split(Constants.U0001)[0];
            
            for(SchemaBean schemaBean : schemaList) {
                if(fileLocation.contains(schemaBean.getHive_db_path())) {
                    String hive_db = schemaBean.getHive_db(); // 数据分片属于哪个hivedatabase
                    String table = schemaBean.getTable(); // 数据分片属于哪个table

                    // 从数据路径获取分区时间
                    String[] pathArray = fileLocation.split(table);
                    String[] dateArray = pathArray[1].split("/");
                    String partitionTime = dateArray[1] + " " + dateArray[2] + ":" + dateArray[3] + ":00";
                    
                    outputKey.set(hive_db, table, unique, partitionTime, 0L);
                    break;
                }
            }
            
            context.write(outputKey, value);
        }
    }

    /**
     * 合并数据reducer
     */
    public static class MergeReducer extends Reducer<DataDistinctBean, Text, NullWritable, Text> {

        private MultipleOutputs<NullWritable, Text> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
        }

        NullWritable outputKey = NullWritable.get();

        @Override
        protected void reduce(DataDistinctBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> valueIterator = values.iterator();
            if(valueIterator.hasNext()) {
                Text lastData = valueIterator.next(); // 按时间降序排序，第一条是最新数据

                String partitionTime = key.getPartitionTime();

                // 解析时间
                Date date = DateUtils.parseTime(StringUtils.isBlank(partitionTime) ? Constants.DEFAUL_TIME : partitionTime);

                String year = String.format(Constants.YEAR, date);
                String month = String.format(Constants.MONTH, date);
                String day = String.format(Constants.DAY, date);

                String hour = String.format(Constants.HOUR, date);
                String min = String.format(Constants.MIN, date);
                String min10 = DateUtils.getMIN10(min); // 取整10分钟

                String outputPartitionPath = Constants.MERGE_DATA + "/" + key.getHivedb() + "/" + key.getTable() + "/" + Constants.PART_DAY + "=" + DateUtils.getTodayDate() + "/" + Constants.PART_HOUR + "=" + hour + "/" + Constants.PART_MIN + "=" + min10 + "/";

                multipleOutputs.write(outputKey, lastData, outputPartitionPath);
            }
        }
    }

}
