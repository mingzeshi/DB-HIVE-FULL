package org.frozen.mr.datadrivendbinputformat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.frozen.bean.loadHiveBean.HiveDataSet;
import org.frozen.constant.Constants;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class FloatSplitter implements DBSplitter_Develop {

	private static final Log LOG = LogFactory.getLog(FloatSplitter.class);

	private static final double MIN_INCREMENT = 10000 * Double.MIN_VALUE;

	public List<InputSplit> split(Configuration conf, ResultSet results, String colName, Map<String, HiveDataSet> hiveDataSetMap) throws SQLException {

		String db = conf.get(Constants.SPLITTERDB);
		String table = conf.get(Constants.SPLITTERTABLE);
		String conditions = conf.get(Constants.SPLITTERCONDITIONS);
		String fields = conf.get(Constants.SPLITTERFIELDS);
		Long batchCount = conf.getLong("map.task.batch.count", 3000000);

		LOG.warn("Generating splits for a floating-point index column. Due to the");
		LOG.warn("imprecise representation of floating-point values in Java, this");
		LOG.warn("may result in an incomplete import.");
		LOG.warn("You are strongly encouraged to choose an integral split column.");

		List<InputSplit> splits = new ArrayList<InputSplit>();

		if (results.getString(1) == null && results.getString(2) == null) {
			// Range is null to null. Return a null split accordingly.
			splits.add(new DataDrivenDBInputFormat_Develop.DataDrivenDBInputSplit_Develop(colName + " IS NULL", colName + " IS NULL", db, table, conditions, fields, hiveDataSetMap));
			return splits;
		}

		double minVal = results.getDouble(1);
		double maxVal = results.getDouble(2);

		// Use this as a hint. May need an extra task if the size doesn't
		// divide cleanly.
		int numSplits = conf.getInt(MRJobConfig.NUM_MAPS, 1);
		double splitSize = (maxVal - minVal) / (double) numSplits;

		if (splitSize < MIN_INCREMENT) {
			splitSize = MIN_INCREMENT;
		}

		String lowClausePrefix = colName + " >= ";
		String highClausePrefix = colName + " < ";

		double curLower = minVal;
		double curUpper = curLower + splitSize;

		while (curUpper < maxVal) {
			splits.add(new DataDrivenDBInputFormat_Develop.DataDrivenDBInputSplit_Develop(lowClausePrefix + Double.toString(curLower), highClausePrefix + Double.toString(curUpper), db, table, conditions, fields, hiveDataSetMap));

			curLower = curUpper;
			curUpper += splitSize;
		}

		// Catch any overage and create the closed interval for the last split.
		if (curLower <= maxVal || splits.size() == 1) {
			splits.add(new DataDrivenDBInputFormat_Develop.DataDrivenDBInputSplit_Develop(lowClausePrefix + Double.toString(curLower), colName + " <= " + Double.toString(maxVal), db, table, conditions, fields, hiveDataSetMap));
		}

		if (results.getString(1) == null || results.getString(2) == null) {
			// At least one extrema is null; add a null split.
			splits.add(new DataDrivenDBInputFormat_Develop.DataDrivenDBInputSplit_Develop(colName + " IS NULL", colName + " IS NULL", db, table, conditions, fields, hiveDataSetMap));
		}

		return splits;
	}
}
