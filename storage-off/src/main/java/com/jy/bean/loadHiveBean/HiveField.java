package com.jy.bean.loadHiveBean;

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
}
