package org.frozen.test.file;

import java.util.List;

import org.frozen.constant.Constants;
import org.frozen.util.FileUtil;

public class ReadWriteFile {

	public static void main(String[] args) {
//		List<String> fileList = FileUtil.readFile("C:\\Users\\Administrator\\Desktop\\文件\\20200520\\files.txt");
//		
//		for(String line : fileList) {
//			System.out.println(line);
//		}
		
		try {
			StringBuffer message = new StringBuffer();
			
			for(int a = 0; a < 10; a++) {
				message.append("bbbbbb" + a);
				message.append(Constants.LINE_N + Constants.LINE_R);
			}
			
			FileUtil.writeFile("C:\\Users\\Administrator\\Desktop\\文件\\20200520\\files.txt", message.toString(), false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
