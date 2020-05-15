package org.frozen.compress;

import java.io.IOException;
import java.util.List;
import java.util.Stack;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.frozen.mr.datadrivendbinputformat.DeleteExistsTextOutputFormat;
import org.frozen.util.HadoopTool;
import org.frozen.util.HadoopUtil;

public class CompressMR extends HadoopTool  {
	
	private final static String MR_BASE_PATH = "mr.base.output.path";
	
	public static void main(String[] args) throws Exception {
        execMain(new CompressMR(), args);
    }
	
	@Override
	public int run(String[] args) throws Exception {

		Configuration configuration = getConf(); // 创建配置信息
		FileSystem fileSystem = FileSystem.get(configuration); // 创建文件系统

		configuration.set(MR_BASE_PATH, args[1]);

		Job job = getJobInstance(configuration);

		job.setJarByClass(CompressMR.class);
	
		job.setMapperClass(CompressMRMapper.class);

		job.setInputFormatClass(TextInputFormat.class);
		
		job.setOutputFormatClass(DeleteExistsTextOutputFormat.class);

		
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		Path hpo = new Path(configuration.get("mr.data.process.other", "/mr_process/other_one/text_file_compress/" + UUID.randomUUID().toString() + "/")); // 这里的目录并不是hive表数据预处理输入的目录，是MultipleOutputs产生的其它文件
		if(HadoopUtil.fileExists(fileSystem, hpo)) { // 如果hive预处理输出其它文件目录已经存在则删除
			HadoopUtil.delete(fileSystem, hpo, true);
		}
		
		String outputCompress = configuration.get("file.output.compress", "bzip2");

		if("bzip2".equals(outputCompress)) {
			FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class); // Bzip2
		} else if("gzip".equals(outputCompress)) {
			FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); // Gzip
		}
		
		Boolean isPath = configuration.getBoolean("isFile", false);
		
		if(isPath) { // 如果input files 在文件中 
			Path[] pathArray = null;
			
			Path file = new Path(args[0]);

			System.out.println("input paths in file, to " + file.toString());
	    	if(HadoopUtil.fileExists(fileSystem, file)) {
	    		List<String> pathStrList = HadoopUtil.readTextFromFile(fileSystem, file);
	    		
	    		pathArray = new Path[pathStrList.size()];
	    		
	    		for(int i = 0; i < pathStrList.size(); i ++) {
	    			String p = pathStrList.get(i);
	    			
	    			if(StringUtils.isNotBlank(p)) {
	    				pathArray[i] = new Path(p);
	    			}
	    		}
	    	} else {
	    		System.err.println("input paths in file, not exists");
	    		return 1;
	    	}

			TextInputFormat.setInputPaths(job, pathArray);
		} else {
			TextInputFormat.setInputPaths(job, args[0]);
		}

		FileOutputFormat.setOutputPath(job, hpo); // 数据处理完成后其它文件输出目录
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class CompressMRMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		
		private MultipleOutputs<NullWritable, Text> multipleOutputs;
		private String basePath;
		private String fileOutPut;
		private String sourceFileName;
		private NullWritable outputKey = NullWritable.get();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration configuration = context.getConfiguration();
			
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
			
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			
			/**
			 * fileSplit.getPath() 可以拿到数据文件的绝对路径
			 * 但是拿到的是hdfs://service/aaa/bbb/ccc/ddd/ 以hdfs开头的路径
			 * 
			 * 我们定义的文件输出目录是: 自定义文件输出目录(args[1]) + hdfs://service/aaa/bbb/ccc/ddd/ = /自定义目录(args[1])/hdfs://service/aaa/bbb/ccc/ddd/ 
			 * 以上这个目录是不能被hdfs定义的，所以我们使用Path.getParent迭代去掉hfds://service/
			 */
			Path parentPath = fileSplit.getPath().getParent(); //文件的父目录
			
			Stack<String> pathStack = new Stack<String>();
			
			while(parentPath != null) {
				
				pathStack.add(parentPath.getName());
				
				parentPath = parentPath.getParent();
			}

			StringBuffer buf = new StringBuffer();
			while(!pathStack.isEmpty()) {
				buf.append(pathStack.pop() + "/");
			}
			
			fileOutPut = buf.toString();
			
			sourceFileName = fileSplit.getPath().getName();
						
			basePath = configuration.get(MR_BASE_PATH);
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			multipleOutputs.write(outputKey, value, basePath + "/" + fileOutPut + "/" + sourceFileName);
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if (multipleOutputs != null) {
				multipleOutputs.close();
			}
		}
	}

}
