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

package org.frozen.mr.sequenceinputformat;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import net.sf.json.JSONObject;

import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.frozen.bean.loadHiveBean.HiveMetastore;
import org.frozen.bean.loadHiveBean.hdfsLoadHiveDWBean.HiveDWDataSet;
import org.frozen.bean.loadHiveBean.hdfsLoadHiveODSBean.HiveODSDataSet;
import org.frozen.util.XmlUtil;

/** A section of an input file.  Returned by {@link
 * InputFormat#getSplits(JobContext)} and passed to
 * {@link InputFormat#createRecordReader(InputSplit,TaskAttemptContext)}. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class OWFileSplit extends InputSplit implements Writable {
  private Path file;
  private long start;
  private long length;
  private String[] hosts;
  private SplitLocationInfo[] hostInfos;
  
  private HiveDWDataSet hiveDWDataSet;
  private HiveODSDataSet hiveODSDataSet;

  public OWFileSplit() {}

  /** Constructs a split with host information
   *
   * @param file the file name
   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process
   * @param hosts the list of hosts containing the block, possibly null
   */
  public OWFileSplit(Path file, long start, long length, String[] hosts, HiveDWDataSet hiveDWDataSet, HiveODSDataSet hiveODSDataSet) {
    this.file = file;
    this.start = start;
    this.length = length;
    this.hosts = hosts;
    this.hiveDWDataSet = hiveDWDataSet;
    this.hiveODSDataSet = hiveODSDataSet;
    
  }
  
  /** Constructs a split with host and cached-blocks information
  *
  * @param file the file name
  * @param start the position of the first byte in the file to process
  * @param length the number of bytes in the file to process
  * @param hosts the list of hosts containing the block
  * @param inMemoryHosts the list of hosts containing the block in memory
  */
	public OWFileSplit(Path file, long start, long length, String[] hosts, String[] inMemoryHosts, HiveDWDataSet hiveDWDataSet, HiveODSDataSet hiveODSDataSet) {
		this(file, start, length, hosts, hiveDWDataSet, hiveODSDataSet);
		hostInfos = new SplitLocationInfo[hosts.length];
		for (int i = 0; i < hosts.length; i++) {
			// because N will be tiny, scanning is probably faster than a
			// HashSet
			boolean inMemory = false;
			for (String inMemoryHost : inMemoryHosts) {
				if (inMemoryHost.equals(hosts[i])) {
					inMemory = true;
					break;
				}
			}
			hostInfos[i] = new SplitLocationInfo(hosts[i], inMemory);
		}
	}
	
	
 
	public HiveDWDataSet getHiveDWDataSet() {
		return hiveDWDataSet;
	}

	public void setHiveDWDataSet(HiveDWDataSet hiveDWDataSet) {
		this.hiveDWDataSet = hiveDWDataSet;
	}

	public HiveODSDataSet getHiveODSDataSet() {
		return hiveODSDataSet;
	}

	public void setHiveYYDataSet(HiveODSDataSet hiveODSDataSet) {
		this.hiveODSDataSet = hiveODSDataSet;
	}

/** The file containing this split's data. */
  public Path getPath() { return file; }
  
  /** The position of the first byte in the file to process. */
  public long getStart() { return start; }
  
  /** The number of bytes in the file to process. */
  @Override
  public long getLength() { return length; }

  @Override
  public String toString() { return file + ":" + start + "+" + length; }

  ////////////////////////////////////////////
  // Writable methods
  ////////////////////////////////////////////

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, file.toString());
    out.writeLong(start);
    out.writeLong(length);
    Text.writeString(out, JSONObject.fromObject(hiveDWDataSet).toString());
    Text.writeString(out, JSONObject.fromObject(hiveODSDataSet).toString());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    file = new Path(Text.readString(in));
    start = in.readLong();
    length = in.readLong();
    hosts = null;
    this.hiveDWDataSet = (HiveDWDataSet) JSONObject.toBean(JSONObject.fromObject(Text.readString(in).toString()), HiveDWDataSet.class);
    this.hiveODSDataSet = (HiveODSDataSet) JSONObject.toBean(JSONObject.fromObject(Text.readString(in).toString()), HiveODSDataSet.class);
    
  }

  @Override
  public String[] getLocations() throws IOException {
    if (this.hosts == null) {
      return new String[]{};
    } else {
      return this.hosts;
    }
  }
  
  @Override
  @Evolving
  public SplitLocationInfo[] getLocationInfo() throws IOException {
    return hostInfos;
  }
}
