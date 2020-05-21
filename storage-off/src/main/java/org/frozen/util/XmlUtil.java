package org.frozen.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;
import org.frozen.bean.importDBBean.ImportRDB_XMLDataSet;
import org.frozen.bean.importDBBean.ImportRDB_XMLDataSetDB;
import org.frozen.bean.loadHiveBean.HiveDataBase;
import org.frozen.bean.loadHiveBean.HiveDataSet;
import org.frozen.bean.loadHiveBean.HiveMetastore;
import org.frozen.constant.Constants;

public class XmlUtil {

//	public static void main(String[] args) {
//		DataSetDB dataSetDB = XmlUtil.parserXml("C:/Users/Administrator/Desktop/文件/20190211/ImportConfiguration.xml");
//		System.out.println(dataSetDB);
//	}
	
	// ----------------------------------------------------------------
	
	/**
	 * 获取XML文件的 Document
	 */
	public static Document loadXML(String xmlF) throws Exception {
		File inputXml = new File(xmlF); // 文件
		SAXReader saxReader = new SAXReader(); // 构建xml文档解析器
		
		return saxReader.read(inputXml); // 加载xml
	}
	
	// ----------------------------------------------------------------
	
	/**
	 * @描述：解析数据导入Hive表-XML配置项文件
	 */
	public static HiveMetastore parserLoadToHiveXML(String xmlF, String type) {
		File inputXml = new File(xmlF); // 文件
		SAXReader saxReader = new SAXReader(); // 构建xml文档解析器
		
		try {
			Document document = saxReader.read(inputXml); // 加载xml
			Element root = document.getRootElement(); // 根节点
		   
			HiveMetastore hiveMetastore = parserHdfsLoadToHiveXMLGetDB(root); // 获取Hive-metastore数据库mysql连接信息

			List<HiveDataBase> dataBaseList = new ArrayList<HiveDataBase>();
			hiveMetastore.setHiveDataBaseList(dataBaseList);
			
			List<Element> dataBaseElements = root.elements(); // 拿到节点下的所有子节点
			for(Element dataBaseElement : dataBaseElements) {
			
				String ennameM = dataBaseElement.attribute("ENNameM").getValue();
				String ennameH = dataBaseElement.attribute("ENNameH").getValue();
				String chname = dataBaseElement.attribute("CHName").getValue();
				String storage = dataBaseElement.attribute("Storage").getValue();
				String description_t = dataBaseElement.attribute("Description").getValue();

				List<HiveDataSet> dataSetList = new ArrayList<HiveDataSet>();
				dataBaseList.add(new HiveDataBase<HiveDataSet>(ennameM, ennameH, chname, storage, description_t, dataSetList));
				
				if(StringUtils.isBlank(type)) { // 是否解析表信息
					List<Element> dataSetElements = dataBaseElement.elements(); // 拿到节点下的所有子节点

					for(Element dataSetElement : dataSetElements) {
						String ennameM_d = dataSetElement.attribute("ENNameM").getValue();
						String ennameH_d = dataSetElement.attribute("ENNameH").getValue();
						String chname_d = dataSetElement.attribute("CHName").getValue();
						String import_append_d = dataSetElement.attribute("Append").getValue();
						String description_d = dataSetElement.attribute("Description").getValue();
		
						dataSetList.add(new HiveDataSet(ennameM_d, ennameH_d, chname_d, import_append_d, description_d, null));
					}
				}
			}

			return hiveMetastore;
		} catch (DocumentException e) {
			System.out.println(e.getMessage());
		}
		return null;
	}
	
	/**
	 * @方法描述：解析导入hdfs业务表xml配置-获取数据库信息
	 * @param root
	 * @return
	 */
	private static HiveMetastore parserHdfsLoadToHiveXMLGetDB(Element root) {
		String enname = root.attribute("ENName").getValue();
		String chname = root.attribute("CHName").getValue();
        String description = root.attribute("Description").getValue();
        
        return new HiveMetastore(enname, chname, description, null);
	}

	// ----------------------------------------------------------------
	
	/**
	 * @描述：解析导入hdfs业务表xml配置：增加只解析db配置项
	 * @param fileName
	 * @param type
	 * @return
	 */
	public static ImportRDB_XMLDataSetDB parserXml(String fileName, String type) {
        File inputXml = new File(fileName);
        SAXReader saxReader = new SAXReader();

        try {
            Document document = saxReader.read(inputXml);
            Element root = document.getRootElement(); // 根节点
            
            if("db".equals(type)) { // 只获取db信息
            	return parserImportXMLGetDB(root);
            } else { // 获取所有信息：db + table
            	return parserImportXML(root);
            }
            
        } catch (DocumentException e) {
            System.out.println(e.getMessage());
        }
        return null;
    }
	
	/**
	 * @方法描述：解析导入hdfs业务表xml配置
	 * @param root
	 * @return
	 */
	private static ImportRDB_XMLDataSetDB parserImportXML(Element root) {

        List<ImportRDB_XMLDataSet> importRDBDataSetList = new ArrayList<ImportRDB_XMLDataSet>();
        
        ImportRDB_XMLDataSetDB importRDBDataSetDB = parserImportXMLGetDB(root);
        importRDBDataSetDB.setImportRDB_XMLDataSet(importRDBDataSetList);

        List<Element> elements = root.elements(); // 拿到节点下的所有子节点
        for(Element element : elements) {
        	String enname = element.attribute("ENName").getValue();
        	String chname = element.attribute("CHName").getValue();
        	String uniqueKey = element.attribute("UniqueKey").getValue();
        	String storage = element.attribute("Storage").getValue();
        	String conditions = element.attribute("Conditions").getValue();

        	String fields = "*";
        	String fieldsValue = element.attribute("Fields").getValue();
        	if(StringUtils.isNotBlank(fieldsValue)) {
        		fields = fieldsValue;
        	}
        	String description_t = element.attribute("Description").getValue();

        	importRDBDataSetList.add(new ImportRDB_XMLDataSet(enname, chname, uniqueKey, storage, conditions, fields, description_t));
        }
        return importRDBDataSetDB;
	}
	
	/**
	 * @方法描述：解析导入hdfs业务表xml配置-获取数据库信息
	 * @param root
	 * @return
	 */
	private static ImportRDB_XMLDataSetDB parserImportXMLGetDB(Element root) {
		String enname = root.attribute("ENName").getValue();
		String chname = root.attribute("CHName").getValue();
        String driver = root.attribute("Driver").getValue();
        String url = root.attribute("Url").getValue();
        String username = root.attribute("UserName").getValue();
        String password = root.attribute("PassWord").getValue();
        String description = root.attribute("Description").getValue();
        
        return new ImportRDB_XMLDataSetDB(enname, chname, driver, url, username, password, description, null);
	}
	
	// ----------------------------------------------------------------

	private static void parser(Element root) {
		System.out.println(root.getName());
		List<Attribute> attributes = root.attributes(); // 拿到节点的所有属性值
        for(Attribute attribute : attributes) { 
        	System.out.println(attribute.getName() + ":" + attribute.getValue());
        }

        List<Element> elements = root.elements(); // 拿到节点下的所有子节点
        for(Element element : elements) {
        	parser(element);
        }
	}

	/**
	 * 将数据写入XML，并将老文件做备份
	 */
	public static void writeXML(String import_db_config_path, List<Element> elements) throws Exception {
		File xmlFile = new File(import_db_config_path);
		if(xmlFile.exists()) { // 将老文件重名名
			xmlFile.renameTo(new File(import_db_config_path + "_bak_" + DateUtils.formatNumber(new Date())));
		}
		
		OutputFormat format = OutputFormat.createPrettyPrint();
		format.setEncoding("UTF-8");
		
		XMLWriter writer = new XMLWriter(
				new OutputStreamWriter(new FileOutputStream(import_db_config_path)), format);

		writer.write(elements);
		writer.close();
	}
}
