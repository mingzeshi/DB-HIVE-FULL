package com.jy.bean.loadHiveBean;

import java.util.List;

/**
 * @描述：hive database bean
 * @author Administrator
 *
 */
public class HiveDataBase<T extends HiveDataSet> {

	private String ennameM; // 业务库-英文名称
	private String ennameH; // hive库-英文名称
	private String chname; // 中文名称
	private String storage; // 所属类型：hive
	private String description; // 描述信息
	private List<T> hiveDataSetList; // mysql表与hive表映射list

	public HiveDataBase() {
	}
	
	public HiveDataBase(String ennameM, String ennameH, String chname, String storage, String description, List<T> hiveDataSetList) {
		this.ennameM = ennameM;
		this.ennameH = ennameH;
		this.chname = chname;
		this.storage = storage;
		this.description = description;
		this.hiveDataSetList = hiveDataSetList;
	}

	public String getEnnameM() {
		return ennameM;
	}

	public void setEnnameM(String ennameM) {
		this.ennameM = ennameM;
	}

	public String getEnnameH() {
		return ennameH;
	}

	public void setEnnameH(String ennameH) {
		this.ennameH = ennameH;
	}

	public String getChname() {
		return chname;
	}

	public void setChname(String chname) {
		this.chname = chname;
	}

	public String getStorage() {
		return storage;
	}

	public void setStorage(String storage) {
		this.storage = storage;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public List<T> getHiveDataSetList() {
		return hiveDataSetList;
	}

	public void setHiveDataSetList(List<T> hiveDataSetList) {
		this.hiveDataSetList = hiveDataSetList;
	}
}
