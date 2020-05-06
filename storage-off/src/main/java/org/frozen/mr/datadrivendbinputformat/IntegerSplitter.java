package org.frozen.mr.datadrivendbinputformat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import org.frozen.bean.loadHiveBean.hdfsLoadHiveDWBean.HiveDWDataSet;
import org.frozen.bean.loadHiveBean.hdfsLoadHiveODSBean.HiveODSDataSet;
import org.frozen.constant.Constants;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class IntegerSplitter implements DBSplitter {
	public List<InputSplit> split(Configuration conf, ResultSet results, String colName, HiveDWDataSet hiveDWDataSet, HiveODSDataSet hiveODSDataSet) throws SQLException {
		String db = conf.get(Constants.SPLITTERDB);
		String table = conf.get(Constants.SPLITTERTABLE);
		String conditions = conf.get(Constants.SPLITTERCONDITIONS);
		String fields = conf.get(Constants.SPLITTERFIELDS);
		Long batchCount = conf.getLong("map.task.batch.count", 3000000);

		long minVal = results.getLong(1);
		long maxVal = results.getLong(2);

		String lowClausePrefix = colName + " >= ";
		String highClausePrefix = colName + " < ";

//		int numSplits = conf.getInt(MRJobConfig.NUM_MAPS, 1);
//		if (numSplits < 1) {
//			numSplits = 1;
//		}

		if (results.getString(1) == null && results.getString(2) == null) {
			// Range is null to null. Return a null split accordingly.
			List<InputSplit> splits = new ArrayList<InputSplit>();
			splits.add(new CustomDataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL", colName + " IS NULL", db, table, conditions, fields, hiveDWDataSet, hiveODSDataSet));
			return splits;
		}
		// Get all the split points together.
		List<Long> splitPoints = split(batchCount, minVal, maxVal);
		List<InputSplit> splits = new ArrayList<InputSplit>();

		// Turn the split points into a set of intervals.
		long start = splitPoints.get(0);
		for (int i = 1; i < splitPoints.size(); i++) {
			long end = splitPoints.get(i);

			if (i == splitPoints.size() - 1) {
				// This is the last one; use a closed interval.
				splits.add(new CustomDataDrivenDBInputFormat.DataDrivenDBInputSplit(lowClausePrefix + Long.toString(start), colName + " <= " + Long.toString(end), db, table, conditions, fields, hiveDWDataSet, hiveODSDataSet));
			} else {
				// Normal open-interval case.
				splits.add(new CustomDataDrivenDBInputFormat.DataDrivenDBInputSplit(lowClausePrefix + Long.toString(start), highClausePrefix + Long.toString(end), db, table, conditions, fields, hiveDWDataSet, hiveODSDataSet));
			}

			start = end;
		}

		if (results.getString(1) == null || results.getString(2) == null) {
			// At least one extrema is null; add a null split.
			splits.add(new CustomDataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL", colName + " IS NULL", db, table, conditions, fields, hiveDWDataSet, hiveODSDataSet));
		}

		return splits;
	}

	List<Long> split(long batchCount, long minVal, long maxVal) throws SQLException {

		List<Long> splits = new ArrayList<Long>();

		// Use numSplits as a hint. May need an extra task if the size doesn't
		// divide cleanly.

//		long splitSize = (maxVal - minVal) / numSplits;
//		if (splitSize < 1) {
//			splitSize = 1;
//		}

		long curVal = minVal;

		while (curVal <= maxVal) {
			splits.add(curVal);
			curVal += batchCount;
		}

		if (splits.get(splits.size() - 1) != maxVal || splits.size() == 1) {
			// We didn't end on the maxVal. Add that to the end of the list.
			splits.add(maxVal);
		}

		return splits;
	}
}
