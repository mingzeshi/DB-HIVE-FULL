package org.frozen.mr;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.frozen.bean.SchemaBean;
import org.frozen.constant.Constants;
import org.frozen.parse.BinLogJsonParse;
import org.frozen.util.CoderUtil;
import org.frozen.util.DateUtils;
import org.frozen.util.HadoopTool;

public class BinlogPartitionMR extends HadoopTool {

    public static void main(String[] args) throws Exception {
        execMain(new BinlogPartitionMR(), args);
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();

        Job job = getJobInstance(configuration);

        job.setJarByClass(BinlogPartitionMR.class); 

        job.setMapperClass(MergeMRMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        
        String distillPath = configuration.get("distill.dir", "/jlc/distill_data/");
        FileOutputFormat.setOutputPath(job, new Path(distillPath + DateUtils.formatNumber(new Date())));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class MergeMRMapper extends Mapper<LongWritable, Text, Text, Text> {

        Map<String, SchemaBean> tableMergeSchema = null;

        private MultipleOutputs<Text, Text> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
//configuration.set("fs.defaultFS", "hdfs://linux01:8020");

            multipleOutputs = new MultipleOutputs<Text, Text>(context);

            try {
                String schemaPath = configuration.get("schema.path", "/jlcstore/datamerge_schema/schema.json");

                FileSystem fileSystem = FileSystem.get(configuration);

                tableMergeSchema = getSchema(fileSystem, schemaPath.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * 从HDFS读取表更新与分区schema
         * @param path
         * @return
         * @throws Exception
         */
        public Map<String, SchemaBean> getSchema(FileSystem fileSystem, String path) throws Exception {
            Map<String, SchemaBean> schemaBeanMap = new HashMap<String, SchemaBean>();

            List<String> lineList = readTextFromFile(fileSystem, new Path(path));

            for(String line : lineList) {
                SchemaBean schemaBean = (SchemaBean) JSONObject.toBean(JSONObject.fromObject(line), SchemaBean.class);
                schemaBeanMap.put(schemaBean.getDb() + "." + schemaBean.getTable(), schemaBean);
            }

            return schemaBeanMap;
        }

        /**
         * 获取json before 或 after value数据
         * @param jsonObject
         * @return
         */
        public String getValues(JSONObject jsonObject, String beforeORafter) {
            try {
                Iterator<JSONObject> jsonArray = jsonObject.getJSONArray(beforeORafter).iterator();
                StringBuffer buffer = new StringBuffer();

                while (jsonArray.hasNext()) {
                    JSONObject jsonObject1 = jsonArray.next();

                    String value = jsonObject1.getString("value");

                    if ("remarks".equals(jsonObject1.getString("name"))) {
                        value = CoderUtil.decoder(value);
                    }

                    buffer.append(value + Constants.U0001);
                }
                return buffer.toString().substring(0, buffer.length() - 1);
            } catch (Exception e) {
                e.printStackTrace();
                return "";
            }
        }

        public String getValues(JSONObject jsonObject) {
            return getValues(jsonObject, "after");
        }

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            try {

                JSONObject jsonObject = JSONObject.fromObject(value.toString());

                JSONObject jsonObjectHead = jsonObject.getJSONObject("head");

                String type = jsonObjectHead.getString("type"); // INSERT UPDATE
                String db = jsonObjectHead.getString("db");
                String table = jsonObjectHead.getString("table");
                String cTime = jsonObjectHead.getString("ctime");
                String execTime = jsonObjectHead.getString("exec_time"); // 执行时间

                String schemaKey = db + "." + table;

                if (tableMergeSchema.containsKey(schemaKey)) { // 判断此条数据是否处理
                    SchemaBean schemaBean = tableMergeSchema.get(schemaKey);

                    String updateColume = schemaBean.getMergecolumn(); // 数据更新字段
                    String partitionTimeColume = schemaBean.getPart_time(); // 分区时间字段
                    String hive_db = schemaBean.getHive_db(); // hive database

                    Map<String, String> schemaMap = BinLogJsonParse.parseColumnNameAndValue(jsonObject, "after", updateColume, partitionTimeColume);

                    String updateId = schemaMap.get(updateColume);
                    String partitionTime = StringUtils.isBlank(schemaMap.get(partitionTimeColume)) ? Constants.DEFAUL_TIME : schemaMap.get(partitionTimeColume); // 数据分区时间

                    // 解析时间
                    Date date = DateUtils.parseTime(partitionTime);

                    String year = String.format(Constants.YEAR, date);
                    String month = String.format(Constants.MONTH, date);
                    String day = String.format(Constants.DAY, date);

                    String hour = String.format(Constants.HOUR, date);
                    String min = String.format(Constants.MIN, date);
                    String min10 = DateUtils.getMIN10(min); // 取整10分钟

                    String outputPartitionPath = Constants.MERGE_DATA + "/" + hive_db + "/" + table + "/" + Constants.PART_DAY + "=" + DateUtils.getTodayDate() + "/" + Constants.PART_HOUR + "=" +hour + "/" + Constants.PART_MIN + "=" + min10 + "/" + schemaKey;

                    // 获取binlog数据
                    outputValue.set(getValues(jsonObject));

                    outputKey.set(hive_db + "." + table + Constants.DATAKV + updateId + Constants.DATAKV + partitionTime + Constants.DATAKV + (StringUtils.isNotBlank(execTime) ? execTime : cTime));

                    multipleOutputs.write(outputKey, outputValue, outputPartitionPath);
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if(multipleOutputs != null) {
                multipleOutputs.close();
            }
        }
    }
}
