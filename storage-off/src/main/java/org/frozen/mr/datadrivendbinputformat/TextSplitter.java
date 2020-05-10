package org.frozen.mr.datadrivendbinputformat;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.frozen.bean.loadHiveBean.HiveDataSet;
import org.frozen.constant.Constants;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TextSplitter extends BigDecimalSplitter {

	private static final Log LOG = LogFactory.getLog(TextSplitter.class);

	public List<InputSplit> split(Configuration conf, ResultSet results, String colName, Map<String, HiveDataSet> hiveDataSetMap) throws SQLException {
		
		String db = conf.get(Constants.SPLITTERDB);
		String table = conf.get(Constants.SPLITTERTABLE);
		String conditions = conf.get(Constants.SPLITTERCONDITIONS);
		String fields = conf.get(Constants.SPLITTERFIELDS);
		Long batchCount = conf.getLong("map.task.batch.count", 3000000);

//		LOG.warn("Generating splits for a textual index column.");
//		LOG.warn("If your database sorts in a case-insensitive order, " + "this may result in a partial import or duplicate records.");
//		LOG.warn("You are strongly encouraged to choose an integral split column.");
		
//		LOG.info("string类型-正在构建数据切片: " + " db: " + db + " table: " + table);
		
		List<InputSplit> splits = new ArrayList<InputSplit>();

		long numSplits = 1;
		
//		numSplits = getNumberSplit(conf, table, batchCount); // 拿到切片数量

		if (1 == numSplits) { // 如果只有一个split
			splits.add(new DataDrivenDBInputFormat_Develop.DataDrivenDBInputSplit_Develop("1=1", "1=1", db, table, conditions, fields, hiveDataSetMap));
			return splits;
	    }

		String minString = results.getString(1);
		String maxString = results.getString(2);

		boolean minIsNull = false;

		if (null == minString) {
			minString = "";
			minIsNull = true;
		}

		if (null == maxString) {
			splits.add(new DataDrivenDBInputFormat_Develop.DataDrivenDBInputSplit_Develop(colName + " IS NULL", colName + " IS NULL", db, table, conditions, fields, hiveDataSetMap));
			return splits;
		}

//		int numSplits = conf.getInt(MRJobConfig.NUM_MAPS, 1);

		String lowClausePrefix = colName + " >= '";
		String highClausePrefix = colName + " < '";

		int maxPrefixLen = Math.min(minString.length(), maxString.length());
		int sharedLen;
		for (sharedLen = 0; sharedLen < maxPrefixLen; sharedLen++) {
			char c1 = minString.charAt(sharedLen);
			char c2 = maxString.charAt(sharedLen);
			if (c1 != c2) {
				break;
			}
		}

		String commonPrefix = minString.substring(0, sharedLen);
		minString = minString.substring(sharedLen);
		maxString = maxString.substring(sharedLen);

		List<String> splitStrings = split(numSplits, minString, maxString, commonPrefix);

		String start = splitStrings.get(0);
		for (int i = 1; i < splitStrings.size(); i++) {
			String end = splitStrings.get(i);

			if (i == splitStrings.size() - 1) {
				splits.add(new DataDrivenDBInputFormat_Develop.DataDrivenDBInputSplit_Develop(lowClausePrefix + start + "'", colName + " <= '" + end + "'", db, table, conditions, fields, hiveDataSetMap));
			} else {
				splits.add(new DataDrivenDBInputFormat_Develop.DataDrivenDBInputSplit_Develop(lowClausePrefix + start + "'", highClausePrefix + end + "'", db, table, conditions, fields, hiveDataSetMap));
			}
		}

		if (minIsNull) {
			splits.add(new DataDrivenDBInputFormat_Develop.DataDrivenDBInputSplit_Develop(colName + " IS NULL", colName + " IS NULL", db, table, conditions, fields, hiveDataSetMap));
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

	List<String> split(Long numSplits, String minString, String maxString, String commonPrefix) throws SQLException {

		BigDecimal minVal = stringToBigDecimal(minString);
		BigDecimal maxVal = stringToBigDecimal(maxString);

		List<BigDecimal> splitPoints = split(new BigDecimal(numSplits), minVal, maxVal);
		List<String> splitStrings = new ArrayList<String>();

		for (BigDecimal bd : splitPoints) {
			splitStrings.add(commonPrefix + bigDecimalToString(bd));
		}

		if (splitStrings.size() == 0 || !splitStrings.get(0).equals(commonPrefix + minString)) {
			splitStrings.add(0, commonPrefix + minString);
		}
		if (splitStrings.size() == 1 || !splitStrings.get(splitStrings.size() - 1).equals(commonPrefix + maxString)) {
			splitStrings.add(commonPrefix + maxString);
		}

		return splitStrings;
	}

	private final static BigDecimal ONE_PLACE = new BigDecimal(65536);

	private final static int MAX_CHARS = 8;

	BigDecimal stringToBigDecimal(String str) {
		BigDecimal result = BigDecimal.ZERO;
		BigDecimal curPlace = ONE_PLACE; // start with 1/65536 to compute the
											// first digit.

		int len = Math.min(str.length(), MAX_CHARS);

		for (int i = 0; i < len; i++) {
			int codePoint = str.codePointAt(i);
			result = result.add(tryDivide(new BigDecimal(codePoint), curPlace));
			curPlace = curPlace.multiply(ONE_PLACE);
		}

		return result;
	}

	String bigDecimalToString(BigDecimal bd) {
		BigDecimal cur = bd.stripTrailingZeros();
		StringBuilder sb = new StringBuilder();

		for (int numConverted = 0; numConverted < MAX_CHARS; numConverted++) {
			cur = cur.multiply(ONE_PLACE);
			int curCodePoint = cur.intValue();
			if (0 == curCodePoint) {
				break;
			}

			cur = cur.subtract(new BigDecimal(curCodePoint));
			sb.append(Character.toChars(curCodePoint));
		}

		return sb.toString();
	}
}
