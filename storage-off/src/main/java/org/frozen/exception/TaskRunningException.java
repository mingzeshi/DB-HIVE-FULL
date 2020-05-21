package org.frozen.exception;


public class TaskRunningException extends BaseException {

	protected TaskRunningException(String code, String msg) {
		super(code, msg);
	}
	
	// ==========异常定义==========
	
	public static final TaskRunningException NO_HIVE_DATASET_EXCEPTION = new TaskRunningException("30001", "MapTask运行异常-无HiveDataSet");
	public static final TaskRunningException NO_REDIS_SPLIT_COUNT_EXCEPTION = new TaskRunningException("30002", "MapTask运行异常-Redis中无此表Split数量");
}