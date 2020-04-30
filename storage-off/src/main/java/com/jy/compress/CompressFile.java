package com.jy.compress;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class CompressFile {
	
	/**
	 * 压缩文件BZip2Codec、GzipCodec、Lz4Codec、SnappyCodec
	 * @param inpath
	 * @param codecClassName
	 * @param output
	 * @throws Exception
	 */
    public static void compress(String inpath, String compressCode, String outpath) throws Exception {
    	String codeClassName = "org.apache.hadoop.io.compress." + compressCode; 
    	Class<?> codecClass = Class.forName(codeClassName);
    	Configuration conf = new Configuration();
    	FileSystem fs = FileSystem.get(conf);
    	CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
    	
    	String suffix = "";
    	
    	if("BZip2Codec".equals(compressCode)) {
    		suffix = ".bz2";
    	} else if("GzipCodec".equals(compressCode)) {
    		suffix = ".gz";
    	}
    	
    	FSDataInputStream in = fs.open(new Path(inpath)); //指定要被压缩的文件路径
    	
    	String outputFilePath = outpath + "/" + inpath;
        
        
        if(inpath.endsWith(suffix)) { // 如果文件已经被压缩，就直接复制到目录目录
        	FSDataOutputStream out = fs.create(new Path(outputFilePath)); //指定压缩文件路径
        	
        	IOUtils.copyBytes(in, out, conf);
        	
        	IOUtils.closeStream(in);
        	IOUtils.closeStream(out);
        } else { // 数据没有被压缩
        	FSDataOutputStream outputStream = fs.create(new Path(outputFilePath + suffix)); //指定压缩文件路径

        	CompressionOutputStream out = codec.createOutputStream(outputStream);

        	IOUtils.copyBytes(in, out, conf);
        	
        	IOUtils.closeStream(in);
        	IOUtils.closeStream(out);
        }        
    }

    /**
     * 使用文件扩展名来推断而来的codec来对文件进行解压缩
     * @param uri
     * @throws IOException
     */
    public static void uncompress(String uri) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        Path inputPath = new Path(uri);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(inputPath);
        if (codec == null) {
            System.out.println("no codec found for " + uri);
            System.exit(1);
        }
        String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
        InputStream in = null;
        OutputStream out = null;
        try {
            in = codec.createInputStream(fs.open(inputPath));
            out = fs.create(new Path(outputUri));
            IOUtils.copyBytes(in, out, conf);
        } finally {
            IOUtils.closeStream(out);
            IOUtils.closeStream(in);
        }
    }

    public static void main(String[] args) throws Exception {
    	
//    	args = new String[] {
//    			"compress-file",
//    			"C:\\Users\\Administrator\\Desktop\\文件\\20200219\\zbb_storage",
//    			"BZip2Codec",
//    			"output"
////    			"300"
//    	};
    	
    	if(args.length <= 0) {
    		help();
    		return;
    	}

        System.out.println("=============[" + new Date() + "]"+args[0]+" begin !!!"+"=============");

        if (args[0].equals("compress")) {
            compress(args[1], args[2], args[3]);
        } else if (args[0].equals("compress-file")) {
        	
        	AtomicInteger globalTaskCount = new AtomicInteger();
        	
        	File file = new File(args[1]);
        	if(file.exists()) {
        		BufferedReader bufferedReader = null;
        		
        		try {
        			
        			bufferedReader = new BufferedReader(new FileReader(file));
            		
        			String filePath = "";
        			List<String> pathList = new ArrayList<String>();
        			
            		while(null != (filePath = bufferedReader.readLine())) {
            			pathList.add(filePath);
            		}
            		
            		int execCount = 100;
            		if(args.length > 4) {
            			execCount = Integer.valueOf(args[4]);
            		}
            		
            		Object[] a = pathList.toArray();
            		
            		int singleTaskNumber = a.length / execCount;
            		
            		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

            		for(int i = 0; i <= singleTaskNumber; i ++) {
            			int begin = i * execCount; // 0 * 100 = 0, 1 * 100 = 100, 2 * 100 = 200 ...
            			int end = begin + execCount;

            			
            			int dataSize = execCount;
            			
            			if(end > a.length) { // 如果被分配的数据结束边界大于总数据数量，则最后一个任务只拷贝剩余部分数据
            				dataSize = a.length % execCount;
            			}
            			
            			String[] elementData = new String[dataSize];
            			System.arraycopy(a, begin, elementData, 0, dataSize);
            			
            			fixedThreadPool.execute(new CompressTask(elementData, args[2], args[3], globalTaskCount)); // 将任务加入线程池，同时可运行任务数是本机CPU的核数
            		}
            		
            		while(true) {
            			try {
							System.out.println(globalTaskCount.get() + " / " + a.length);
							
							Thread.sleep(1000 * 10);
							
							if (globalTaskCount.get() >= a.length) {
								System.out.println(globalTaskCount.get() + " / " + a.length);
								
								fixedThreadPool.shutdown();
								
								break;
							} 
						} catch (Exception e) {
							e.printStackTrace();
						}
            		}            		
        		} catch(Exception e) {
        			e.printStackTrace();
        		} finally {
        			try {
	        			if(bufferedReader != null) {
	        				bufferedReader.close();
	        			}
        			} catch(Exception e) {
        				e.printStackTrace();
        			}
        		}
        		
        	} else {
        		System.out.println("包含需要压缩文件(包含多个文件完整路径)文件不存在");
        		help();
        	}
        } else if (args[0].equals("uncompress"))
            uncompress(args[1]);
        else {
            help();
            return;
        }
        System.out.println("=============[" + new Date() + "]" + args[0] + " over !!!" + "=============");
    }
    
    private static void help() {// | Lz4Codec | SnappyCodec
    	System.err.println("数据  压缩 : yarn jar xxx.jar hdfs.io.compress compress [inpath] [compress type : BZip2Codec | GzipCodec] [outpath]");
    	System.err.println("数据  压缩 : yarn jar xxx.jar hdfs.io.compress compress-file [inpath in textfile] [compress type : BZip2Codec | GzipCodec] [outpath] [single thread execution compress-file, default 100]");
        System.err.println("数据解压缩 : yarn jar xxx.jar hdfs.io.compress uncompress [inpath]");
    }
    
}


class CompressTask implements Runnable {
	
	String[] compressPaths;
	String compressCode;
	String output;
	AtomicInteger sucCount;
	
	public CompressTask(String[] compressPaths, String compressCode, String output, AtomicInteger sucCount) {
		this.compressPaths = compressPaths;
		this.compressCode = compressCode;
		this.output = output;
		this.sucCount = sucCount;
	}

	@Override
	public void run() {
		for(String path : compressPaths) {
			try {
				CompressFile.compress(path, compressCode, output);
				sucCount.incrementAndGet(); // 任务完成计数
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
}
