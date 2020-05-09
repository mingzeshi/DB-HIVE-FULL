package org.frozen.exec;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;

import org.frozen.bean.importDBBean.ImportRDB_XMLDataSet;
import org.frozen.bean.importDBBean.ImportRDB_XMLDataSetDB;
import org.frozen.util.XmlUtil;

public class Generate_odsANDodsXML {
	
	public static void main(String[] args) {
		ImportRDB_XMLDataSetDB dataSetDB = XmlUtil.parserXml("C:/Users/Administrator/Desktop/文件/20190321/ImportConfig.xml", null); // 获取配置文件需要导入的所有表
		String db = dataSetDB.getEnname();
		List<ImportRDB_XMLDataSet> dataSetList = dataSetDB.getImportRDB_XMLDataSet();

		List<InputSplit> splits = new ArrayList<InputSplit>();

		// <DataSet ENNameM="" ENNameH="" CHName="" Description=""></DataSet>
		
		System.out.println("------START------");
		for (ImportRDB_XMLDataSet dataSet : dataSetList) { // 循环每张表
			String tableName = dataSet.getEnname();
//			String splitCol = dataSet.getUniqueKey(); // 主键
//			String conditions = dataSet.getConditions();
//			String fields = dataSet.getFields(); // 数据切片查询字段，默认 *
			
			String str = "<DataSet ENNameM=\"" + tableName + "\" ENNameH=\"" + tableName + "\" CHName=\"\" Description=\"\"></DataSet>";
			System.out.println(str);
		}
		System.out.println("------END------");
	}

}
