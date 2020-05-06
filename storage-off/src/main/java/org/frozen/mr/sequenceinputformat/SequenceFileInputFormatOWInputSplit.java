package org.frozen.mr.sequenceinputformat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import com.google.common.base.Stopwatch;
import org.frozen.bean.loadHiveBean.HiveMetastore;
import org.frozen.bean.loadHiveBean.hdfsLoadHiveDWBean.HiveDWDataSet;
import org.frozen.bean.loadHiveBean.hdfsLoadHiveODSBean.HiveODSDataSet;
import org.frozen.util.XmlUtil;

public class SequenceFileInputFormatOWInputSplit<K, V> extends SequenceFileInputFormat<K, V> {

	private static final Log LOG = LogFactory.getLog(SequenceFileInputFormatOWInputSplit.class);

	private static final double SPLIT_SLOP = 1.1; // 10% slop
	
	@Override
	public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new SequenceFileRecordReader<K, V>();
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		Configuration configuration = job.getConfiguration();
		
		HiveMetastore hiveDWMetastore = XmlUtil.parserHdfsLoadToHiveDWXML(configuration.get("hdfs.load.hive.dw.path"), null); // 获取近源数据快照层配置文件相关信息
		HiveMetastore hiveODSMetastore = XmlUtil.parserHdfsLoadToHiveODSXML(configuration.get("hdfs.load.hive.ods.path"), null); // 获取近源数据应用层配置文件相关信息
		
		/**
		 * 拿到快照层配置项
		 */
		List<HiveDWDataSet> hiveDWDataSetList = hiveDWMetastore.getHiveDataBaseList().get(0).getHiveDataSetList();
		Map<String, HiveDWDataSet> hiveDWTabMap = new HashMap<String, HiveDWDataSet>();
		for(HiveDWDataSet dwDataSet : hiveDWDataSetList) {
			hiveDWTabMap.put(dwDataSet.getEnnameM().toLowerCase(), dwDataSet);
		}

		/**
		 * 拿到应用层配置项
		 */
		List<HiveODSDataSet> hiveODSdataSetList = hiveODSMetastore.getHiveDataBaseList().get(0).getHiveDataSetList();
		Map<String, HiveODSDataSet> hiveODSTabMap = new HashMap<String, HiveODSDataSet>();
		for(HiveODSDataSet odsDataSet : hiveODSdataSetList) {
			hiveODSTabMap.put(odsDataSet.getEnnameM().toLowerCase(), odsDataSet);
		}

		Stopwatch sw = new Stopwatch().start();
		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		long maxSize = getMaxSplitSize(job);

		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);
		for (FileStatus file : files) {
			Path path = file.getPath();
			
			String tableName = path.getParent().getName().toLowerCase(); // 业务表名
			
			HiveDWDataSet hiveDWDataSet = null;
			HiveODSDataSet hiveODSDataSet = null;
			
			
			boolean isLoadData = false;
			if(hiveDWTabMap.containsKey(tableName)) {
				hiveDWDataSet = hiveDWTabMap.get(tableName);
				isLoadData = true;
			}
			if(hiveODSTabMap.containsKey(tableName)) {
				hiveODSDataSet = hiveODSTabMap.get(tableName);
				isLoadData = true;
			}
			
			if(isLoadData) { // 是否加载数据切片
				long length = file.getLen();
				if (length != 0) {
					BlockLocation[] blkLocations;
					if (file instanceof LocatedFileStatus) {
						blkLocations = ((LocatedFileStatus) file).getBlockLocations();
					} else {
						FileSystem fs = path.getFileSystem(job.getConfiguration());
						blkLocations = fs.getFileBlockLocations(file, 0, length);
					}
					if (isSplitable(job, path)) {
						long blockSize = file.getBlockSize();
						long splitSize = computeSplitSize(blockSize, minSize, maxSize);
	
						long bytesRemaining = length;
						while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
							int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
							splits.add(makeSplit(path, length - bytesRemaining, splitSize, blkLocations[blkIndex].getHosts(), blkLocations[blkIndex].getCachedHosts(), hiveDWDataSet, hiveODSDataSet));
							bytesRemaining -= splitSize;
						}
	
						if (bytesRemaining != 0) {
							int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);
							splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining, blkLocations[blkIndex].getHosts(), blkLocations[blkIndex].getCachedHosts(), hiveDWDataSet, hiveODSDataSet));
						}
					} else { // not splitable
						splits.add(makeSplit(path, 0, length, blkLocations[0].getHosts(), blkLocations[0].getCachedHosts(), hiveDWDataSet, hiveODSDataSet));
					}
				} else {
					// Create empty hosts array for zero length files
					splits.add(makeSplit(path, 0, length, new String[0], hiveDWDataSet, hiveODSDataSet));
				}
			}
		}
		// Save the number of input files for metrics/loadgen
		job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
		sw.stop();
		if (LOG.isDebugEnabled()) {
			LOG.debug("Total # of splits generated by getSplits: " + splits.size() + ", TimeTaken: " + sw.elapsedMillis());
		}
		return splits;
	}
	
	protected OWFileSplit makeSplit(Path file, long start, long length, String[] hosts, HiveDWDataSet hiveDWDataSet, HiveODSDataSet hiveODSDataSet) {
		return new OWFileSplit(file, start, length, hosts, hiveDWDataSet, hiveODSDataSet);
	}

	protected OWFileSplit makeSplit(Path file, long start, long length, String[] hosts, String[] inMemoryHosts, HiveDWDataSet hiveDWDataSet, HiveODSDataSet hiveODSDataSet) {
		return new OWFileSplit(file, start, length, hosts, inMemoryHosts, hiveDWDataSet, hiveODSDataSet);
	}
	
}
