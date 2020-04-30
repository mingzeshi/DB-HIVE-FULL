package com.jy.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class TestC {

	public static void main(String[] args) {
/*		
		args = new String[] {
				"",
				"C:\\Users\\Administrator\\Desktop\\文件\\20200219\\zbb_storage",
				"",
				"",
//				"300"
		};
		
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

        		for(int i = 0; i <= singleTaskNumber; i ++) {
        			int begin = i * execCount; // 0 * 100 = 0, 1 * 100 = 100, 2 * 100 = 200 ...
        			int end = begin + execCount;

        			
        			int dataSize = execCount;
        			
        			if(end > a.length) { // 如果被分配的数据结束边界大于总数据数量，则最后一个任务只拷贝剩余部分数据
        				dataSize = a.length % execCount;
        			}
        			
        			String[] elementData = new String[dataSize];
        			System.arraycopy(a, begin, elementData, 0, dataSize);
        			
        			for(String path : elementData) {
        				System.out.println(path);
        			}
        			System.out.println("====================================");
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
    	}
*/
		AtomicInteger count = new AtomicInteger();
		
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		
		for(int i = 0; i < 100; i ++) {
			fixedThreadPool.execute(new Task(count));
		}

		while(true) {
			try {
				fixedThreadPool.shutdown();

				Thread.sleep(100);

				System.out.println(fixedThreadPool.isShutdown());
				
				System.out.println(count.get());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
}

class Task implements Runnable {
	
	AtomicInteger count;
	
	public Task(AtomicInteger count) {
		this.count = count;
	}

	@Override
	public void run() {
		try {
			this.count.incrementAndGet();
			Thread.sleep(500);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
}
