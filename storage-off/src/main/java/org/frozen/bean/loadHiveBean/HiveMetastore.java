package org.frozen.bean.loadHiveBean;

import java.util.List;

/**
 * hive元数据信息所在数据库的一些数据库配置信息
 *
 */
public class HiveMetastore {

	private String enname; // 数据库英文名称
	private String chname; // 数据库中文名称
	private String driver; // 数据库驱动
	private String url; // 数据库连接信息
	private String username; // 用户名
	private String password; // 密码
	private String description; // 描述
	private List<HiveDataBase> hiveDataBaseList; // hive database list

	public HiveMetastore() {
	}

	public HiveMetastore(String enname, String chname, String driver, String url, String username, String password, String description, List<HiveDataBase> hiveDataBaseList) {
		this.enname = enname;
		this.chname = chname;
		this.driver = driver;
		this.url = url;
		this.username = username;
		this.password = password;
		this.description = description;
		this.hiveDataBaseList = hiveDataBaseList;
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

	public List<HiveDataBase> getHiveDataBaseList() {
		return hiveDataBaseList;
	}

	public void setHiveDataBaseList(List<HiveDataBase> hiveDataBaseList) {
		this.hiveDataBaseList = hiveDataBaseList;
	}
}
