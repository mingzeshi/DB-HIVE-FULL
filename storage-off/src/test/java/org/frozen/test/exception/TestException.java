package org.frozen.test.exception;

import org.frozen.exception.BaseException;
import org.frozen.exception.BuildDriverCommonException;
import org.frozen.exception.ImportDBToHiveException;

public class TestException {
	
	public static void main(String[] args) {
		try {
//			throw BuildDriverCommonException.NO_CONFIG_FILE;
//			throw ImportDBToHiveException.NO_EXPORT_CONFIG;
			throw ImportDBToHiveException.NO_HIVE_TAB_CONFIG_EXCEPTION;
		} catch(BaseException b) {
			System.out.println(b.getCode());
			System.out.println(b.getMsg());
		}
	}

}
