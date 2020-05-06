package org.frozen.bean.loadHiveBean.hdfsLoadHiveDWBean;

import java.util.List;
import org.frozen.bean.loadHiveBean.HiveDataSet;

public class HiveDWDataSet extends HiveDataSet {
	private List<HiveDWField> hiveDWFieldList; // hive快照层需要映射的字段

	public HiveDWDataSet() {
	}

	public HiveDWDataSet(String ennameM, String ennameH, String chname, String description, List<HiveDWField> hiveDWFieldList) {
		this.ennameM = ennameM;
		this.ennameH = ennameH;
		this.chname = chname;
		this.description = description;
		this.hiveDWFieldList = hiveDWFieldList;
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

	public List<HiveDWField> getHiveDWFieldList() {
		return hiveDWFieldList;
	}

	public void setHiveDWFieldList(List<HiveDWField> hiveDWFieldList) {
		this.hiveDWFieldList = hiveDWFieldList;
	}	
}
