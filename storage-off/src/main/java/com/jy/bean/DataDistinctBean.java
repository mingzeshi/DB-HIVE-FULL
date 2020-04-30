package com.jy.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataDistinctBean implements WritableComparable<DataDistinctBean> {
	
	public DataDistinctBean() {}

    private String hivedb; // hive database
    private String table; // 表名
    private String uniqueId; // 唯一去重ID
    private String partitionTime; // 分区时间
    private Long execTime; // binlog 触发时间

    public void set(String hivedb, String table, String uniqueId, String partitionTime, Long execTime) {
        this.hivedb = hivedb;
        this.table = table;
        this.uniqueId = uniqueId;
        this.partitionTime = partitionTime;
        this.execTime = execTime;
    }

    public int compareTo(DataDistinctBean dataDistinctBean) {
        return dataDistinctBean.getExecTime().compareTo(this.getExecTime());
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(hivedb);
        dataOutput.writeUTF(table);
        dataOutput.writeUTF(uniqueId);
        dataOutput.writeUTF(partitionTime);
        dataOutput.writeLong(execTime);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.hivedb = dataInput.readUTF();
        this.table = dataInput.readUTF();
        this.uniqueId = dataInput.readUTF();
        this.partitionTime = dataInput.readUTF();
        this.execTime = dataInput.readLong();
    }

    // -----------------------------------

    public String getHivedb() {
        return hivedb;
    }

    public void setHivedb(String hivedb) {
        this.hivedb = hivedb;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public void setUniqueId(String uniqueId) {
        this.uniqueId = uniqueId;
    }

    public String getPartitionTime() {
        return partitionTime;
    }

    public void setPartitionTime(String partitionTime) {
        this.partitionTime = partitionTime;
    }

    public Long getExecTime() {
        return execTime;
    }

    public void setExecTime(Long execTime) {
        this.execTime = execTime;
    }
}
