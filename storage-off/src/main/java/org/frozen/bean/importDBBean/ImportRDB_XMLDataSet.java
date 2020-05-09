package org.frozen.bean.importDBBean;

/**
 * 表信息bean
 * @author Administrator
 *
 */
public class ImportRDB_XMLDataSet {

	private String enname; // 英文名称
	private String chname; // 中文名称
	private String uniqueKey; // 主键
	private String storage; // 所属类型：mysql oracle
	private String conditions; // 条件配置项
	private String fields; // 字段配置项
	private String description; // 描述信息

	public ImportRDB_XMLDataSet() {
	}

	public ImportRDB_XMLDataSet(String enname, String chname, String uniqueKey, String storage, String conditions, String fields, String description) {
		this.enname = enname;
		this.chname = chname;
		this.uniqueKey = uniqueKey;
		this.storage = storage;
		this.conditions = conditions;
		this.fields = fields;
		this.description = description;
	}

	public String getEnname() {
		return enname;
	}

	public void setEnname(String enname) {
		this.enname = enname;
	}

	public String getChname() {
		return chname;
	}

	public void setChname(String chname) {
		this.chname = chname;
	}

	public String getUniqueKey() {
		return uniqueKey;
	}

	public void setUniqueKey(String uniqueKey) {
		this.uniqueKey = uniqueKey;
	}

	public String getStorage() {
		return storage;
	}

	public void setStorage(String storage) {
		this.storage = storage;
	}

	public String getConditions() {
		return conditions;
	}

	public void setConditions(String conditions) {
		this.conditions = conditions;
	}

	public String getFields() {
		return fields;
	}

	public void setFields(String fields) {
		this.fields = fields;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}	
}
