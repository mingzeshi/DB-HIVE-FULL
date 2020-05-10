package org.frozen.mr.datadrivendbinputformat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.frozen.bean.loadHiveBean.HiveDataSet;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface DBSplitter_Develop {
  List<InputSplit> split(Configuration conf, ResultSet results, String colName, Map<String, HiveDataSet> hiveDataSetMap) throws SQLException;
}
