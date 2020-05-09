package org.frozen.bean.loadHiveBean;

import java.util.List;

/**
 * @描述：mysql表与hive表映射相关信息bean
 */
public class HiveDataSet {
	protected String ennameM; // mysql-英文名称
	protected String ennameH; // hive-英文名称
	protected String chname; // 中文名称
	protected String description; // 描述信息
	private List<HiveField> hiveODSFieldList; // hive应用层需要映射的字段

	public HiveDataSet() {
	}

	public HiveDataSet(String ennameM, String ennameH, String chname, String description, List<HiveField> hiveODSFieldList) {
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

	public List<HiveField> getHiveODSFieldList() {
		return hiveODSFieldList;
	}

	public void setHiveODSFieldList(List<HiveField> hiveODSFieldList) {
		this.hiveODSFieldList = hiveODSFieldList;
	}
}
