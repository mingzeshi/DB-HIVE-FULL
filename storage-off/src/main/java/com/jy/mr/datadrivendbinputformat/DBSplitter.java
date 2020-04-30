package com.jy.mr.datadrivendbinputformat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import com.jy.bean.loadHiveBean.hdfsLoadHiveDWBean.HiveDWDataSet;
import com.jy.bean.loadHiveBean.hdfsLoadHiveODSBean.HiveODSDataSet;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface DBSplitter {
  List<InputSplit> split(Configuration conf, ResultSet results, String colName, HiveDWDataSet hiveDWDataSet, HiveODSDataSet hiveODSDataSet) throws SQLException;
}
