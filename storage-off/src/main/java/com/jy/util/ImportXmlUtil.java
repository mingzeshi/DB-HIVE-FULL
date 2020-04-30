//package com.jy.util;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.run.ayena.mgr.metadata.DataSet;
//import com.run.ayena.mgr.metadata.Field;
//import com.run.ayena.mgr.policy.unique.Dimension;
//import com.run.ayena.mgr.policy.unique.Dimensions;
//import com.run.ayena.mgr.policy.unique.PolicyFile;
//import com.run.ayena.mgr.policy.unique.StatResult;
//import com.run.ayena.mgr.policy.unique.StatResults;
//import com.run.ayena.mgr.policy.unique.TimeKey;
//import com.run.ayena.mgr.policy.unique.Unique;
//import com.run.ayena.objectpp.conf.metadata.AyenaDataSetFile;
//import com.run.ayena.objectpp.util.ConstantsUtil;
//import com.run.ayena.objectpp.util.XmlUtils;
//import com.jy.bean.DataSetDB;
//import com.jy.bean.FilterBean;
//import com.jy.bean.ImportDBFilterBean;
//
//public class ImportXmlUtil {
//
//	private static final Logger log = LoggerFactory.getLogger(ImportXmlUtil.class);
//	
//	private static Map<String, ImportDBFilterBean> importDBMap = new HashMap<String, ImportDBFilterBean>();
//	
//	public static Map<String, Map<String, String>> getImportMapByXML(String xmlPath) throws IOException {
//		
//		
//		
//		List<Unique> unique = policyFile.getUnique();
//		for (Unique mu : unique) {
//			try {
//				String namespace = mu.getNamespace(); // EXT003
//				// 外码转内码
//				AyenaDataSetFile dsFile = AyenaDataSetFile.getInstance(namespace); // dataset.xml中数据实体管理
//				String outterDataSet = mu.getDataSet();//外码 WA_BASIC_0006
//				DataSet ds = dsFile.getDataSetByDSID(outterDataSet);//数据集实体
//				String aliasDSID = ds.getAliasDSID();//内码 WA_BASIC_0006
//				String key = namespace+ConstantsUtil.SPECIALCHAR+aliasDSID;//各种数据词典的key	EXT003,WA_BASIC_0006
//				
//				Dimensions dimensions = mu.getDimensions();  // 去重策略字段：Unique文件中<Dimensions></Dimensions>	例如：F010008/B050016/G020004
//				TimeKey timeKey = mu.getTimeKey();	// 获取<Timekey Name="B050014" /> 最近采集时间
//				StatResults statRes =  mu.getStatResults();	// <StatResults><StatResults/> 例如：B050012首次采集时间/B050014最近采集时间/B050015发现次数/B050018累计发现天数
//				Set<String> firstNetSet = new HashSet<String>();
//				Set<String> lastNetSet = new HashSet<String>();
//				if (dimensions != null && timeKey != null && statRes != null) {
//					//<Dimensions>
//					List<Dimension> dimension = dimensions.getDimension(); // 所有的去重策略字段
//					for(Dimension dim : dimension) {
//						String storeFieldId = dsFile.getStoreFieldId(outterDataSet, dim.getElement());	// 传入：WA_BASIC_0006, B050016    返回：RB050016
//						dim.setElement(storeFieldId);
//					}
//					//<StatResults>
//					List<StatResult> stat = statRes.getStatResult(); // 统计结果字段的逻辑对应关系
//					for(StatResult s : stat){
//						String storeFieldId = dsFile.getStoreFieldId(outterDataSet, s.getElement());// 传入：WA_BASIC_0006, B050016    返回：RB050016
//						s.setElement(storeFieldId);	
//						if(s.getStatItem().trim().toUpperCase().endsWith("FIRST_TIME")){
//							firstTimeMap.put(key, storeFieldId);
//						}else if(s.getStatItem().trim().toUpperCase().endsWith("LAST_TIME")){
//							lastTimeMap.put(key, storeFieldId);
//						}else if(s.getStatItem().trim().toUpperCase().endsWith("COUNTER")){
//							countMap.put(key, storeFieldId);
//						}else if(s.getStatItem().trim().toUpperCase().endsWith("TOTAL_DAY")){
//							totalDayMap.put(key, storeFieldId);
//						}else if(s.getStatItem().trim().toUpperCase().endsWith("FIRST")){
//							firstNetSet.add(storeFieldId);
//						}else if(s.getStatItem().trim().toUpperCase().endsWith("LAST")){
//							lastNetSet.add(storeFieldId);
//						}
//					}
//					firstMap.put(key, firstNetSet);
//					lastMap.put(key, lastNetSet);
//					//<TimeKey>
//					String timeKeyName = timeKey.getName(); // B050014
//					if(!timeKeyName.equals(ConstantsUtil.SYSTEMTIME)){
//						String aliansFileId = dsFile.getStoreFieldId(outterDataSet, timeKeyName);// 传入：WA_BASIC_0006, B050014    返回：RB050014
//						timeKey.setName(aliansFileId);
//						
//					}
//					//获取timeKeyName的数据类型	
//					for(Field field : ds.getField()){ // 数据实体，遍历每一个Field(xml:行数据)
//						//时间字段，时间类型字典初始化
//						if(field.getElementID().equals(timeKeyName)){ // 定位Field
//							timeKeyTypeMap.put(key, field.getValueType()); // 数据类型：timestamp、string
//						}
//						//数据集中，多值字段词典初始化
//						if(field.isBeMultiValue()){ // 属性：BeMultiValue="false"
//							if(multiValueMap.containsKey(key)){
//								Set<String> multiSet = multiValueMap.get(key);
//								multiSet.add(field.getAliasEID());
//							}else{
//								Set<String> multiSet = new HashSet<String>();
//								multiSet.add(field.getAliasEID());
//								multiValueMap.put(key, multiSet);
//							}
//						}
//					}					
//					uniqMap.put(namespace + ConstantsUtil.SPECIALCHAR + aliasDSID, mu); // (EXT003,WA_BASIC_0006), Unique(每一个去重策略)
//				} else {
//					log.error("UniqueXmlUtil.getUniqueMapByNmAndDs,error set for dataset : " + namespace + "."
//							+ outterDataSet);
//				}
//			
//			} catch (Exception e) {
//				log.error("UniqueXmlUtil.getUniqueMapByNmAndDs,find a Unique.xml error : " + e.getMessage());
//			}
//			
//		}
//		log.warn("***finish loading 'Unique.xml'***");
//		return uniqMap;
//	}
//
//	public static boolean isMultiValue(String multiKey,String fieldName){
//		Set<String> multiValueSet = multiValueMap.get(multiKey);
//		if(multiValueSet == null){
//			return false;
//		}
//		return multiValueSet.contains(fieldName);
//	}
//	
//	public static Logger getLog() {
//		return log;
//	}
//}
