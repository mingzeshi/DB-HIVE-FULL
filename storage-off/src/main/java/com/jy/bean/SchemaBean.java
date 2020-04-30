package com.jy.bean;

public class SchemaBean {

    private String db; // mysql database
    private String hive_db; // hive database
    private String hive_db_path; // hive database在hdfs绝对路径
    private String table; // 表名
    private String mergecolumn; // 数据合并字段
    private String part_time; // 分区时间字段

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getHive_db() {
        return hive_db;
    }

    public void setHive_db(String hive_db) {
        this.hive_db = hive_db;
    }

    public String getHive_db_path() {
        return hive_db_path;
    }

    public void setHive_db_path(String hive_db_path) {
        this.hive_db_path = hive_db_path;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getMergecolumn() {
        return mergecolumn;
    }

    public void setMergecolumn(String mergecolumn) {
        this.mergecolumn = mergecolumn;
    }

    public String getPart_time() {
        return part_time;
    }

    public void setPart_time(String part_time) {
        this.part_time = part_time;
    }

    @Override
    public String toString() {
        return db + "\t" + hive_db + "\t" + hive_db_path + "\t" + table + "\t" + mergecolumn + "\t" + part_time;
    }
}
