package org.frozen.bean.loadHiveBean;

import java.util.List;

import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 * @描述：mysql表与hive表映射相关信息bean
 */
public class HiveDataSet {
	protected String ennameM; // mysql-英文名称
	protected String ennameH; // hive-英文名称
	protected String chname; // 中文名称
	protected String description; // 描述信息
	private List<HiveField> hiveFieldList; // Hive表需要映射的字段

	public HiveDataSet() {
	}

	public HiveDataSet(String ennameM, String ennameH, String chname, String description, List<HiveField> hiveFieldList) {
		this.ennameM = ennameM;
		this.ennameH = ennameH;
		this.chname = chname;
		this.description = description;
		this.hiveFieldList = hiveFieldList;
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

	public List<HiveField> getHiveFieldList() {
		return hiveFieldList;
	}

	public void setHiveODSFieldList(List<HiveField> hiveFieldList) {
		this.hiveFieldList = hiveFieldList;
	}
	
	@Override
	public String toString() {
		return "<DataSet ENNameM=\"" + ennameM + "\" ENNameH=\"" + ennameH + "\" CHName=\"" + chname + "\" Description=\"" + description + "\"></DataSet>";
	}
	
	/**
	 * 创建XML节点
	 */
	public Element toXMLElement() {
		Element price = DocumentHelper.createElement("DataSet");
		price.addAttribute("ENNameM", ennameM);
		price.addAttribute("ENNameH", ennameH);
		price.addAttribute("CHName", chname);
		price.addAttribute("Description", description);
	
		return price;
	}
}
