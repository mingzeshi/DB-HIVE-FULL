package org.frozen.bean.loadHiveBean;

/**
 * @描述：mysql表与hive字段映射相关信息bean
 * @author Administrator
 */
public class HiveField {
	protected String ennameM; // mysql字段-英文名称
	protected String ennameH; // hive字段-英文名称
	protected String chname; // 中文名称
	protected String where; // 过滤条件
	protected String description; // 描述信息
	private String ennameT; // 数据转换字段
	private String operation; // 数据转换

	public HiveField(String ennameM, String ennameH, String ennameT, String chname, String where, String description, String operation) {
		this.ennameM = ennameM;
		this.ennameH = ennameH;
		this.ennameT = ennameT;
		this.chname = chname;
		this.where = where;
		this.description = description;
		this.operation = operation;
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

	public String getOperation() {
		return operation;
	}

	public void setOperation(String operation) {
		this.operation = operation;
	}

	public String getEnnameT() {
		return ennameT;
	}

	public void setEnnameT(String ennameT) {
		this.ennameT = ennameT;
	}
}
