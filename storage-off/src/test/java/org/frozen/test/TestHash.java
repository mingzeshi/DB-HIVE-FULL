package org.frozen.test;

import java.io.FileWriter;

public class TestHash {

	public static void main(String[] args) {
		StringBuffer buf = new StringBuffer();
		// 30521423
		for(int i = 0; i < 30521423; i++) {
			int hash = hash(i);
			int n = 128, num;
			num = (n - 1) & hash;

			buf.append(num + "\t");
			
			if(buf.length() > 1000) {
				try {
					FileWriter write = new FileWriter("D:/mr_file/20181011/aaa.txt", true);
					write.write(buf.toString() + "\n");
					buf = new StringBuffer();
					write.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}
		}
		System.out.println("------end------");
	}
	
	
	
	public static int hash(Object key) {
        int h;
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }
	
}
