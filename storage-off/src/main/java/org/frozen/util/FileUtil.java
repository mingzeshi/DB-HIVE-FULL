package org.frozen.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class FileUtil {
	
	/**
	 * 读文件
	 * 
	 * @param filePath
	 * @return
	 */
	public static List<String> readFile(String filePath) {
		List<String> dataList = new ArrayList<String>();
		
		InputStream inputStream = null;
		InputStreamReader reader = null;
		BufferedReader br = null;
		
		try {
			File filename = new File(filePath); // 要读取以上路径的input。txt文件
			inputStream = new FileInputStream(filename);
			reader = new InputStreamReader(inputStream); // 建立一个输入流对象reader
			br = new BufferedReader(reader);
			String line = "";
			while ((line = br.readLine()) != null) {
				dataList.add(line);
			} 
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (br != null) {
					br.close();
				}
				if(reader != null) {
					reader.close();
				}
				if(inputStream != null) {
					inputStream.close();
				}
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
		return dataList;
	}
	
	/**
	 * 写文件
	 * 
	 * @param filePath
	 * @param message
	 * @throws Exception
	 */
	public static void writeFile(String filePath, String message, Boolean append) throws Exception {
		FileWriter fileWriter = null;
		BufferedWriter out = null;
		try {
			File writename = new File(filePath);
			
			fileWriter = new FileWriter(writename, append);
			out = new BufferedWriter(fileWriter);
			out.write(message); // \r\n即为换行
			
			out.flush(); // 把缓存区内容压入文件
		} catch(Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (out != null) {
					out.close();
				}
				if (fileWriter != null) {
					fileWriter.close();
				} 
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
	}
}
