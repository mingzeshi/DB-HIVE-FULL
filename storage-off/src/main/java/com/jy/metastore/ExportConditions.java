package com.jy.metastore;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat.Condition;
import com.alibaba.druid.util.JdbcConstants;
import com.jy.bean.importDBBean.ImportRDBDataSet;
import com.jy.bean.importDBBean.ImportRDBDataSetDB;
import com.jy.bean.loadHiveBean.HiveDataBase;
import com.jy.bean.loadHiveBean.HiveMetastore;
import com.jy.bean.loadHiveBean.hdfsLoadHiveDWBean.HiveDWDataSet;
import com.jy.util.JDBCUtil;
import com.jy.util.XmlUtil;

public class ExportConditions {
	public static void main(String[] args) {
		String importConfigPath = System.getProperty("impConfP"); // ImportConfig.xml 路径
		String loadToODSPath = System.getProperty("odsConfP"); // LoadToHiveODS.xml
		String loadToStoragePath = System.getProperty("stoConfP"); // LoadToHiveStorage.xml
		
		if(StringUtils.isBlank(importConfigPath) || StringUtils.isBlank(loadToODSPath) || StringUtils.isBlank(loadToStoragePath)) {
			System.out.println("------------------------------------------------------");
			System.out.println("		-DimpConfP=xxx : 任务接入mysql表xml配置文件");
			System.out.println("		-DodsConfP=xxx : ods任务xml配置文件路径");
			System.out.println("		-DstoConfP=xxx : storage任务xml配置文件路径");
			System.out.println("------------------------------------------------------");
			
			return;
		}
		
		ImportRDBDataSetDB importRDBDataSetDB = XmlUtil.parserXml(importConfigPath, "tables");
		
		Map<String, String> hiveTablesODS = readXML(loadToODSPath);
		Map<String, String> hiveTablesStorage = readXML(loadToStoragePath);
		
		List<ImportRDBDataSet> importRDBDataSet = importRDBDataSetDB.getImportRDBDataSet();
		
		for(ImportRDBDataSet dataSet : importRDBDataSet) {
			String conditions = dataSet.getConditions();
			if(StringUtils.isNotBlank(conditions)) {
				String dbTab = dataSet.getEnname();
				
				String odsTab = hiveTablesODS.get(dbTab);
				String storageTab = hiveTablesStorage.get(dbTab);
				
				// create_time > date_sub(curdate(),interval 1 day) or update_time > date_sub(curdate(),interval 1 day)
				
				System.out.println(odsTab + "|" + storageTab + "|" + parserSQLCon(conditions));				
			}
		}
	}
	
	private static String parserSQLCon(String conditions) {
		String sql = "select * from f where " + conditions;
		String dbType = JdbcConstants.MYSQL;

		// 格式化输出
		String result = SQLUtils.format(sql, dbType);
		List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
		
		StringBuffer buf = new StringBuffer();

		// 解析出的独立语句的个数
		for (int i = 0; i < stmtList.size(); i++) {

			SQLStatement stmt = stmtList.get(i);
			MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
			stmt.accept(visitor);

			List<Condition> condition = visitor.getConditions();
			
			for(Condition con : condition) {
				buf.append(con.getColumn() + ",");
			}
		}
		
		return buf.toString().substring(0, buf.length() - 1);
	}
	
	private static Map<String, String> readXML(String xmlPath) {

		Map<String, String> hiveTableSchema = new HashMap<String, String>();

		HiveMetastore hiveMetastore = XmlUtil.parserHdfsLoadToHiveDWXML(xmlPath, null); // 获取配置文件数据库相关信息

		Connection connection = JDBCUtil.getConn(hiveMetastore.getDriver(), hiveMetastore.getUrl(), hiveMetastore.getUsername(), hiveMetastore.getPassword());

		List<HiveDataBase> dataBaseList = hiveMetastore.getHiveDataBaseList();
		
		for (HiveDataBase<HiveDWDataSet> dataBase : dataBaseList) {
			String hiveName = dataBase.getEnnameH();

			List<HiveDWDataSet> dataSetList = dataBase.getHiveDataSetList();

			for (HiveDWDataSet dataSet : dataSetList) {

				String keyM = dataSet.getEnnameM();
				String keyH = dataSet.getEnnameH();

				List<String> vlauesArray = JDBCUtil.getHiveTabColumns(connection, dataBase.getEnnameH().toLowerCase(), dataSet.getEnnameH().toLowerCase());

				Set<String> valuesHashSet = new HashSet<String>();

				for (String field : vlauesArray) {
					valuesHashSet.add(field.toLowerCase());
				}

				hiveTableSchema.put(keyM, hiveName + "." + keyH);
			}
		}

		return hiveTableSchema;
	}
}
