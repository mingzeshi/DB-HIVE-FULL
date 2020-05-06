package org.frozen.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import org.frozen.bean.importDBBean.ImportRDBDataSet;
import org.frozen.bean.importDBBean.ImportRDBDataSetDB;
import org.frozen.bean.loadHiveBean.HiveDataBase;
import org.frozen.bean.loadHiveBean.HiveMetastore;
import org.frozen.bean.loadHiveBean.hdfsLoadHiveDWBean.HiveDWDataSet;
import org.frozen.bean.loadHiveBean.hdfsLoadHiveODSBean.HiveODSDataSet;

public class XmlUtil {

//	public static void main(String[] args) {
//		DataSetDB dataSetDB = XmlUtil.parserXml("C:/Users/Administrator/Desktop/文件/20190211/ImportConfiguration.xml");
//		System.out.println(dataSetDB);
//	}
	
	// ----------------------------------------------------------------
	
	/**
	 * @描述：解析将数据由hdfs导入到hive快照层配置项
	 * @param fileName
	 * @param type
	 * @return
	 */
	public static HiveMetastore parserHdfsLoadToHiveDWXML(String fileName, String type) {
		File inputXml = new File(fileName);
        SAXReader saxReader = new SAXReader();
        
        try {
            Document document = saxReader.read(inputXml);
            Element root = document.getRootElement(); // 根节点
           
            HiveMetastore hiveMetastore = parserHdfsLoadToHiveXMLGetDB(root); // 获取hive-metastore数据库mysql连接信息
            List<HiveDataBase> dataBaseList = new ArrayList<HiveDataBase>();
            hiveMetastore.setHiveDataBaseList(dataBaseList);
            
            List<Element> dataBaseElements = root.elements(); // 拿到节点下的所有子节点
            for(Element dataBaseElement : dataBaseElements) {
            
            	String ennameM = dataBaseElement.attribute("ENNameM").getValue();
            	String ennameH = dataBaseElement.attribute("ENNameH").getValue();
	            String chname = dataBaseElement.attribute("CHName").getValue();
	            String storage = dataBaseElement.attribute("Storage").getValue();
	            String description_t = dataBaseElement.attribute("Description").getValue();
	
	            List<HiveDWDataSet> mhmDataSetList = new ArrayList<HiveDWDataSet>();
	            dataBaseList.add(new HiveDataBase<HiveDWDataSet>(ennameM, ennameH, chname, storage, description_t, mhmDataSetList));
	            
	            if(StringUtils.isBlank(type)) { // 是否解析表信息
		            List<Element> dataSetElements = dataBaseElement.elements(); // 拿到节点下的所有子节点
	
		            for(Element dataSetElement : dataSetElements) {
		            	String ennameM_d = dataSetElement.attribute("ENNameM").getValue();
		            	String ennameH_d = dataSetElement.attribute("ENNameH").getValue();
		            	String chname_d = dataSetElement.attribute("CHName").getValue();
		            	String description_d = dataSetElement.attribute("Description").getValue();
		
		            	mhmDataSetList.add(new HiveDWDataSet(ennameM_d, ennameH_d, chname_d, description_d, null));
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
	 * @描述：解析将数据由hdfs导入到hive应用层配置项
	 * @param fileName
	 * @param type
	 * @return
	 */
	public static HiveMetastore parserHdfsLoadToHiveODSXML(String fileName, String type) {
		File inputXml = new File(fileName);
		SAXReader saxReader = new SAXReader();
		
		try {
			Document document = saxReader.read(inputXml);
			Element root = document.getRootElement(); // 根节点
			
			HiveMetastore hiveMetastore = parserHdfsLoadToHiveXMLGetDB(root); // 获取hive-metastore数据库mysql连接信息
			List<HiveDataBase> dataBaseList = new ArrayList<HiveDataBase>();
			hiveMetastore.setHiveDataBaseList(dataBaseList);
			
				List<Element> dataBaseElements = root.elements(); // 拿到节点下的所有子节点
				for(Element dataBaseElement : dataBaseElements) {
					
					String ennameM = dataBaseElement.attribute("ENNameM").getValue();
					String ennameH = dataBaseElement.attribute("ENNameH").getValue();
					String chname = dataBaseElement.attribute("CHName").getValue();
					String storage = dataBaseElement.attribute("Storage").getValue();
					String description_t = dataBaseElement.attribute("Description").getValue();
					
					List<HiveODSDataSet> mhmDataSetList = new ArrayList<HiveODSDataSet>();
					dataBaseList.add(new HiveDataBase<HiveODSDataSet>(ennameM, ennameH, chname, storage, description_t, mhmDataSetList));

					if(StringUtils.isBlank(type)) { // 是否解析表信息
					
						List<Element> dataSetElements = dataBaseElement.elements(); // 拿到节点下的所有子节点
						
						for(Element dataSetElement : dataSetElements) {
							String ennameM_d = dataSetElement.attribute("ENNameM").getValue();
							String ennameH_d = dataSetElement.attribute("ENNameH").getValue();
							String chname_d = dataSetElement.attribute("CHName").getValue();
							String description_d = dataSetElement.attribute("Description").getValue();
							
							mhmDataSetList.add(new HiveODSDataSet(ennameM_d, ennameH_d, chname_d, description_d, null));
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
        String driver = root.attribute("Driver").getValue();
        String url = root.attribute("Url").getValue();
        String username = root.attribute("UserName").getValue();
        String password = root.attribute("PassWord").getValue();
        String description = root.attribute("Description").getValue();
        
        return new HiveMetastore(enname, chname, driver, url, username, password, description, null);
	}

	// ----------------------------------------------------------------
	
	/**
	 * @描述：解析导入hdfs业务表xml配置：增加只解析db配置项
	 * @param fileName
	 * @param type
	 * @return
	 */
	public static ImportRDBDataSetDB parserXml(String fileName, String type) {
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
	private static ImportRDBDataSetDB parserImportXML(Element root) {

        List<ImportRDBDataSet> importRDBDataSetList = new ArrayList<ImportRDBDataSet>();
        
        ImportRDBDataSetDB importRDBDataSetDB = parserImportXMLGetDB(root);
        importRDBDataSetDB.setImportRDBDataSet(importRDBDataSetList);

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

        	importRDBDataSetList.add(new ImportRDBDataSet(enname, chname, uniqueKey, storage, conditions, fields, description_t));
        }

        return importRDBDataSetDB;
	}
	
	/**
	 * @方法描述：解析导入hdfs业务表xml配置-获取数据库信息
	 * @param root
	 * @return
	 */
	private static ImportRDBDataSetDB parserImportXMLGetDB(Element root) {
		String enname = root.attribute("ENName").getValue();
		String chname = root.attribute("CHName").getValue();
        String driver = root.attribute("Driver").getValue();
        String url = root.attribute("Url").getValue();
        String username = root.attribute("UserName").getValue();
        String password = root.attribute("PassWord").getValue();
        String description = root.attribute("Description").getValue();
        
        return new ImportRDBDataSetDB(enname, chname, driver, url, username, password, description, null);
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
}
