/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.frozen.test.custom;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBSplitter;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.db.DataDrivenDBInputFormat;

/**
 * A InputFormat that reads input data from an SQL table.
 * Operates like DBInputFormat, but instead of using LIMIT and OFFSET to demarcate
 * splits, it tries to generate WHERE clauses which separate the data into roughly
 * equivalent shards.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DataDrivenDBInputFormatC<T extends DBWritable> extends DataDrivenDBInputFormat<T> {

  private static final Log LOG = LogFactory.getLog(DataDrivenDBInputFormatC.class);

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {

//    int targetNumTasks = job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
//    if (1 == targetNumTasks) {
//      // There's no need to run a bounding vals query; just return a split
//      // that separates nothing. This can be considerably more optimal for a
//      // large table with no index.
//      List<InputSplit> singletonSplit = new ArrayList<InputSplit>();
//      singletonSplit.add(new DataDrivenDBInputSplit("1=1", "1=1"));
//      return singletonSplit;
//    }

	  job.getConfiguration().setInt(MRJobConfig.NUM_MAPS, 5);
	  
    ResultSet results = null;
    Statement statement = null;
    try {
      statement = connection.createStatement();

      results = statement.executeQuery(getBoundingValsQuery());
      results.next();

      // Based on the type of the results, use a different mechanism
      // for interpolating split points (i.e., numeric splits, text splits,
      // dates, etc.)
      int sqlDataType = results.getMetaData().getColumnType(1);
      DBSplitter splitter = getSplitter(sqlDataType);
      if (null == splitter) {
        throw new IOException("Unknown SQL data type: " + sqlDataType);
      }

      return splitter.split(job.getConfiguration(), results, getDBConf().getInputOrderBy());
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    } finally {
      // More-or-less ignore SQL exceptions here, but log in case we need it.
      try {
        if (null != results) {
          results.close();
        }
      } catch (SQLException se) {
        LOG.debug("SQLException closing resultset: " + se.toString());
      }

      try {
        if (null != statement) {
          statement.close();
        }
      } catch (SQLException se) {
        LOG.debug("SQLException closing statement: " + se.toString());
      }

      try {
        connection.commit();
        closeConnection();
      } catch (SQLException se) {
        LOG.debug("SQLException committing split transaction: " + se.toString());
      }
    }
  }
  
	public static void setInput(Job job, Class<? extends DBWritable> inputClass, String tableName, String conditions, String splitBy, String... fieldNames) {
		DBInputFormat.setInput(job, inputClass, tableName, conditions, splitBy, fieldNames);
		job.setInputFormatClass(DataDrivenDBInputFormatC.class);
	}
}
