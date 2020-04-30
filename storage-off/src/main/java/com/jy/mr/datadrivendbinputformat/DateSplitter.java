package com.jy.mr.datadrivendbinputformat;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;

import com.jy.bean.loadHiveBean.hdfsLoadHiveDWBean.HiveDWDataSet;
import com.jy.bean.loadHiveBean.hdfsLoadHiveODSBean.HiveODSDataSet;
import com.jy.constant.Constants;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DateSplitter extends IntegerSplitter {

	private static final Log LOG = LogFactory.getLog(DateSplitter.class);

	public List<InputSplit> split(Configuration conf, ResultSet results, String colName, HiveDWDataSet hiveDWDataSet, HiveODSDataSet hiveODSDataSet) throws SQLException {
		String db = conf.get(Constants.SPLITTERDB);
		String table = conf.get(Constants.SPLITTERTABLE);
		String fields = conf.get(Constants.SPLITTERFIELDS);
		String conditions = conf.get(Constants.SPLITTERCONDITIONS);
		Long batchCount = conf.getLong("map.task.batch.count", 3000000);
		
		LOG.info("date类型-正在构建数据切片: " + " db: " + db + " table: " + table);

		long minVal;
		long maxVal;

		int sqlDataType = results.getMetaData().getColumnType(1);
		minVal = resultSetColToLong(results, 1, sqlDataType);
		maxVal = resultSetColToLong(results, 2, sqlDataType);

		String lowClausePrefix = colName + " >= ";
		String highClausePrefix = colName + " < ";

		int numSplits = getNumberSplit(conf, table, batchCount);
//		int numSplits = 5;

		if (minVal == Long.MIN_VALUE && maxVal == Long.MIN_VALUE) {
			// The range of acceptable dates is NULL to NULL. Just create a
			// single split.
			List<InputSplit> splits = new ArrayList<InputSplit>();
			splits.add(new CustomDataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL", colName + " IS NULL", db, table, conditions, fields, hiveDWDataSet, hiveODSDataSet));
			return splits;
		}

		// Gather the split point integers
		List<Long> splitPoints = split(numSplits, minVal, maxVal);
		List<InputSplit> splits = new ArrayList<InputSplit>();

		// Turn the split points into a set of intervals.
		long start = splitPoints.get(0);
		Date startDate = longToDate(start, sqlDataType);
		if (sqlDataType == Types.TIMESTAMP) {
			// The lower bound's nanos value needs to match the actual
			// lower-bound nanos.
			try {
				((java.sql.Timestamp) startDate).setNanos(results.getTimestamp(1).getNanos());
			} catch (NullPointerException npe) {
				// If the lower bound was NULL, we'll get an NPE; just ignore it
				// and don't set nanos.
			}
		}

		for (int i = 1; i < splitPoints.size(); i++) {
			long end = splitPoints.get(i);
			Date endDate = longToDate(end, sqlDataType);

			if (i == splitPoints.size() - 1) {
				if (sqlDataType == Types.TIMESTAMP) {
					// The upper bound's nanos value needs to match the actual
					// upper-bound nanos.
					try {
						((java.sql.Timestamp) endDate).setNanos(results.getTimestamp(2).getNanos());
					} catch (NullPointerException npe) {
						// If the upper bound was NULL, we'll get an NPE; just
						// ignore it and don't set nanos.
					}
				}
				// This is the last one; use a closed interval.
				splits.add(new CustomDataDrivenDBInputFormat.DataDrivenDBInputSplit(lowClausePrefix + dateToString(startDate), colName + " <= " + dateToString(endDate), db, table, conditions, fields, hiveDWDataSet, hiveODSDataSet));
			} else {
				// Normal open-interval case.
				splits.add(new CustomDataDrivenDBInputFormat.DataDrivenDBInputSplit(lowClausePrefix + dateToString(startDate), highClausePrefix + dateToString(endDate), db, table, conditions, fields, hiveDWDataSet, hiveODSDataSet));
			}

			start = end;
			startDate = endDate;
		}

		if (minVal == Long.MIN_VALUE || maxVal == Long.MIN_VALUE) {
			// Add an extra split to handle the null case that we saw.
			splits.add(new CustomDataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL", colName + " IS NULL", db, table, conditions, fields, hiveDWDataSet, hiveODSDataSet));
		}

		return splits;
	}
	
	/**
	 * 计算切片数量
	 */
	private int getNumberSplit(Configuration conf, String table, long batchCount) {
		try {
			DBConfiguration dbConf = new DBConfiguration(conf);
			Connection connection = null;
			try {
				connection = dbConf.getConnection();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			Statement statementC = connection.createStatement();
			ResultSet resultsC = statementC.executeQuery("select count(0) from " + table);
			resultsC.next();
			Integer dataCount = Integer.valueOf(resultsC.getString(1));
			
			
			Integer numSplits = 1;
			if(dataCount > (batchCount * 1.2)) {
				BigDecimal bigDecimal = new BigDecimal(dataCount).divide(new BigDecimal(batchCount), 2);			
				String[] nums = String.valueOf(bigDecimal.doubleValue()).split(Constants.SPOT);
				Long num = Long.parseLong(nums[1]);
				if(num > 0) {
					numSplits = Integer.parseInt(nums[0]) + 1;
				} else {
					numSplits = Integer.parseInt(nums[0]);
				}
			}
			
			return numSplits;
		} catch (Exception e) {
			e.printStackTrace();
			return 1;
		}
	}

	private long resultSetColToLong(ResultSet rs, int colNum, int sqlDataType) throws SQLException {
		try {
			switch (sqlDataType) {
			case Types.DATE:
				return rs.getDate(colNum).getTime();
			case Types.TIME:
				return rs.getTime(colNum).getTime();
			case Types.TIMESTAMP:
				return rs.getTimestamp(colNum).getTime();
			default:
				throw new SQLException("Not a date-type field");
			}
		} catch (NullPointerException npe) {
			// null column. return minimum long value.
			LOG.warn("Encountered a NULL date in the split column. Splits may be poorly balanced.");
			return Long.MIN_VALUE;
		}
	}

	/** Parse the long-valued timestamp into the appropriate SQL date type. */
	private Date longToDate(long val, int sqlDataType) {
		switch (sqlDataType) {
		case Types.DATE:
			return new java.sql.Date(val);
		case Types.TIME:
			return new java.sql.Time(val);
		case Types.TIMESTAMP:
			return new java.sql.Timestamp(val);
		default: // Shouldn't ever hit this case.
			return null;
		}
	}

	protected String dateToString(Date d) {
		return "'" + d.toString() + "'";
	}
	
	List<Long> split(long numSplits, long minVal, long maxVal) throws SQLException {

		List<Long> splits = new ArrayList<Long>();

		long splitSize = (maxVal - minVal) / numSplits;
		if (splitSize < 1) {
			splitSize = 1;
		}

		long curVal = minVal;

		while (curVal <= maxVal) {
			splits.add(curVal);
			curVal += splitSize;
		}

		if (splits.get(splits.size() - 1) != maxVal || splits.size() == 1) {
			// We didn't end on the maxVal. Add that to the end of the list.
			splits.add(maxVal);
		}

		return splits;
	}
}
