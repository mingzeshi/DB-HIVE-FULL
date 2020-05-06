package org.frozen.bean.loadHiveBean.hdfsLoadHiveODSBean;

import java.util.List;

import org.frozen.bean.loadHiveBean.HiveDataSet;

public class HiveODSDataSet extends HiveDataSet {
	private List<HiveODSField> hiveODSFieldList; // hive应用层需要映射的字段

	public HiveODSDataSet() {
	}

	public HiveODSDataSet(String ennameM, String ennameH, String chname, String description, List<HiveODSField> hiveODSFieldList) {
		this.ennameM = ennameM;
		this.ennameH = ennameH;
		this.chname = chname;
		this.description = description;
		this.hiveODSFieldList = hiveODSFieldList;
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

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public List<HiveODSField> getHiveODSFieldList() {
		return hiveODSFieldList;
	}

	public void setHiveODSFieldList(List<HiveODSField> hiveODSFieldList) {
		this.hiveODSFieldList = hiveODSFieldList;
	}
}
