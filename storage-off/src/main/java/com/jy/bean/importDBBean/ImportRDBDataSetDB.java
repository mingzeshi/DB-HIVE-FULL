package com.jy.bean.importDBBean;

import java.util.List;

/**
 * 数据库信息bean
 * 
 * @author Administrator
 *
 */
public class ImportRDBDataSetDB {

	private String enname; // 数据库英文名称
	private String chname; // 数据库中文名称
	private String driver; // 数据库驱动
	private String url; // 数据库连接信息
	private String username; // 用户名
	private String password; // 密码
	private String description; // 描述
	private List<ImportRDBDataSet> importRDBDataSet; // 数据库下表集合

	public ImportRDBDataSetDB() {
	}

	public ImportRDBDataSetDB(String enname, String chname, String driver, String url, String username, String password, String description, List<ImportRDBDataSet> importRDBDataSet) {
		this.enname = enname;
		this.chname = chname;
		this.driver = driver;
		this.url = url;
		this.username = username;
		this.password = password;
		this.description = description;
		this.importRDBDataSet = importRDBDataSet;
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

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public List<ImportRDBDataSet> getImportRDBDataSet() {
		return importRDBDataSet;
	}

	public void setImportRDBDataSet(List<ImportRDBDataSet> importRDBDataSet) {
		this.importRDBDataSet = importRDBDataSet;
	}

}
