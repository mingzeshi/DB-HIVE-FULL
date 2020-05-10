package org.frozen.mr.datadrivendbinputformat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.frozen.bean.loadHiveBean.HiveDataSet;
import org.frozen.constant.Constants;

/**
 * Implement DBSplitter over boolean values.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BooleanSplitter implements DBSplitter_Develop {
	public List<InputSplit> split(Configuration conf, ResultSet results, String colName, Map<String, HiveDataSet> hiveDataSetMap) throws SQLException {
		
		String db = conf.get(Constants.SPLITTERDB);
		String table = conf.get(Constants.SPLITTERTABLE);
		String conditions = conf.get(Constants.SPLITTERCONDITIONS);
		String fields = conf.get(Constants.SPLITTERFIELDS);
		Long batchCount = conf.getLong("map.task.batch.count", 3000000);

		List<InputSplit> splits = new ArrayList<InputSplit>();

		if (results.getString(1) == null && results.getString(2) == null) {
			// Range is null to null. Return a null split accordingly.
			splits.add(new DataDrivenDBInputFormat_Develop.DataDrivenDBInputSplit_Develop(colName + " IS NULL", colName + " IS NULL", db, table, conditions, fields, hiveDataSetMap));
			return splits;
		}

		boolean minVal = results.getBoolean(1);
		boolean maxVal = results.getBoolean(2);

		// Use one or two splits.
		if (!minVal) {
			splits.add(new DataDrivenDBInputFormat_Develop.DataDrivenDBInputSplit_Develop(colName + " = FALSE", colName + " = FALSE", db, table, conditions, fields, hiveDataSetMap));
		}

		if (maxVal) {
			splits.add(new DataDrivenDBInputFormat_Develop.DataDrivenDBInputSplit_Develop(colName + " = TRUE", colName + " = TRUE", db, table, conditions, fields, hiveDataSetMap));
		}

		if (results.getString(1) == null || results.getString(2) == null) {
			// Include a null value.
			splits.add(new DataDrivenDBInputFormat_Develop.DataDrivenDBInputSplit_Develop(colName + " IS NULL", colName + " IS NULL", db, table, conditions, fields, hiveDataSetMap));
		}

		return splits;
	}
}
