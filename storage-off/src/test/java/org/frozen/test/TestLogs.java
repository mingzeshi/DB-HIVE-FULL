package org.frozen.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLogs {
	
	private static final Logger log = LoggerFactory.getLogger(TestLogs.class);
	private static final Logger outputPath = LoggerFactory.getLogger("checkN_TAB_COL");
	
	public static void main(String[] args) {
		
		log.info("----------START----------");
		
		boolean isBreak = false; 
		
		while(!isBreak) {
			outputPath.info("");
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				e.printStackTrace();
			}
			outputPath.info("11111111111111111");
		}
		
		log.info("----------END----------");
		
	}

}
