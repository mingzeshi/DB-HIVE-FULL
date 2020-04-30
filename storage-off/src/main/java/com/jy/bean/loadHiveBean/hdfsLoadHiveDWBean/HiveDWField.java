package com.jy.bean.loadHiveBean.hdfsLoadHiveDWBean;

import com.jy.bean.loadHiveBean.HiveField;

public class HiveDWField extends HiveField {

	public HiveDWField(String ennameM, String ennameH, String chname, String where, String description) {
		this.ennameM = ennameM;
		this.ennameH = ennameH;
		this.chname = chname;
		this.where = where;
		this.description = description;
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
	public String getWhere() {
		return where;
	}
	public void setWhere(String where) {
		this.where = where;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}

}
