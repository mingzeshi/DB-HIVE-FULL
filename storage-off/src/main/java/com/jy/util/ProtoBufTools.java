package com.jy.util;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.Parser;
import com.jy.constant.Constants;
import com.jy.pb.ObjectData.ObjectAttr;
import com.jy.pb.ObjectData.ObjectBase;
import com.jy.pb.ObjectData.ObjectBaseAttr;
import com.jy.pb.ObjectData.ObjectCounter;
import com.jy.pb.ObjectData.ObjectInfo;
import com.jy.pb.ObjectData.ObjectInfoAttr;

/*
import com.run.ayena.objectpp.conf.metadata.AyenaDataSetFile;
import com.run.ayena.objectpp.performance.ObjectDistillPfMetric;
*/

/**
 * protobuf 工具类
 * @author lunkai
 *
 */

public class ProtoBufTools {
	
	private static final Logger log = LoggerFactory.getLogger(ProtoBufTools.class);
	 
	private static Map<String,Method> methodMap = Maps.newHashMap();
	private static Map<String,Method> parserMethodMap = Maps.newHashMap();
	private static Map<String,Parser<?>> parserMap = Maps.newHashMap();
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	
	/**
	 * Protobuf对象字段间的复值（copy vlaue）
	 * 将源protobuf对象srcfield字段的值  赋值 到目标protobuf对象的desfield字段中
	 * 
	 * @param srcObject 源对象
	 * @param srcfield  源属性名称
	 * @param desSrc    目标对象
	 * @param desfield  目标属性名称
	 * @return GeneratedMessage  返回目标对象
	 */
	/*
	@Deprecated
	public static GeneratedMessage copyValue(GeneratedMessage srcObject , String srcfield, 
											  GeneratedMessage desObject , String desfield){
		Descriptor  desDescriptor = null;
		FieldDescriptor desFieldDescriptor=null;
		Builder desBuilder = null;
		try {
			ProtobufPair pbp = getMessageValue(srcObject,srcfield);
			Object value = pbp.getValue();			
			desDescriptor = desObject.getDescriptorForType();
			desFieldDescriptor = desDescriptor.findFieldByName(desfield);
			desBuilder = desObject.toBuilder();
			if("INT".equals(desFieldDescriptor.getJavaType().name().toString())){
				desBuilder.setField(desFieldDescriptor, Integer.parseInt(value.toString()));				
			}else if("LONG".equals(desFieldDescriptor.getJavaType().name().toString())){
				desBuilder.setField(desFieldDescriptor, Long.parseLong(value.toString()));
			}else if("FLOAT".equals(desFieldDescriptor.getJavaType().name().toString())){
				desBuilder.setField(desFieldDescriptor, Float.parseFloat(value.toString()));				
			}else if("DOUBLE".equals(desFieldDescriptor.getJavaType().name().toString())){
				desBuilder.setField(desFieldDescriptor, Double.parseDouble(value.toString()));				
			}else {
				desBuilder.setField(desFieldDescriptor, value);				
			}
			desObject = (GeneratedMessage) desBuilder.setField(desFieldDescriptor, value).build();
		} catch (IllegalArgumentException  e) {
			log.error("IllegalArgumentException:srcBuilder="+srcObject.getClass().getName()
					+";desBuilder="+desObject.getClass().getName()
					+";srcfield="+srcfield
					+";desfield="+desfield);
			e.printStackTrace();		
		} catch (Exception e) {
			log.error("Exception:srcBuilder="+srcObject.getClass().getName()
					+";desBuilder="+desObject.getClass().getName()
					+";srcfield="+srcfield
					+";desfield="+desfield);
			e.printStackTrace();			
		} finally{
			desBuilder=null;	
			desFieldDescriptor=null;
			desDescriptor=null;		
		}
		return desObject;
	}
	*/
	
	/**
	 * Protobuf对象builder字段间的复值（copy vlaue）
	 * 将源对象srcfield字段的值  赋值 到目标对象的desfield字段中
	 * @param srcBuilder 源builder
	 * @param srcfield   源字段名称
	 * @param desBuilder 目标builder
	 * @param desfield   目标字段名称
	 */
	/*
	public static void copyValue(Message.Builder srcBuilder, String srcfield,Message.Builder desBuilder, String desfield,
							String namespace, String aliasDSID) {
		try {
			ProtobufPair pbp = getMessageValue(srcBuilder, srcfield);
			Object value = pbp.getValue();
			if(value==null || value.toString().isEmpty()){
				return ;
			}
			FieldDescriptor srcFD=pbp.getFieldDescriptor();			
			Descriptor desDescriptor = desBuilder.getDescriptorForType();
			FieldDescriptor desFD = desDescriptor.findFieldByName(desfield);
			//格式转换
			value = convertJavaType(desFD,srcFD,value,namespace,aliasDSID);
			desBuilder.setField(desFD, value);
		} catch (IllegalArgumentException e) {
			log.error("IllegalArgumentException:srcBuilder="+srcBuilder.getClass().getName()
					+";desBuilder="+desBuilder.getClass().getName()
					+";srcfield="+srcfield
					+";desfield="+desfield);
			e.printStackTrace();
		} catch (Exception e) {
			log.error("Exception:srcBuilder="+srcBuilder.getClass().getName()
					+";desBuilder="+desBuilder.getClass().getName()
					+";srcfield="+srcfield
					+";desfield="+desfield);
			e.printStackTrace();
		}
	}
	*/
	
	/**
	 * Protobuf对象builder字段间的复值（copy vlaue）
	 * 将源对象srcfield字段的值  赋值 到目标对象的desfield字段中
	 * @param srcBuilder 源builder
	 * @param srcfield   源字段名称
	 * @param desBuilder 目标builder
	 * @param desfield   目标字段名称
	 */
	/*
	public static void copyValueUseDefault(Message.Builder srcBuilder, String srcfield,
										Message.Builder desBuilder, String desfield, String defaultValue) {
		try {
			ProtobufPair pbp = getMessageValueUseDefault(srcBuilder, srcfield, defaultValue);
			Object value = pbp.getValue();
			FieldDescriptor srcFD=pbp.getFieldDescriptor();			
			Descriptor desDescriptor = desBuilder.getDescriptorForType();
			FieldDescriptor desFD = desDescriptor.findFieldByName(desfield);
			//格式转换
			value = convertJavaType(desFD,srcFD,value,null,null);
			desBuilder.setField(desFD, value);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	*/
	
	/**
	 * 获取protobuf对象中field字段的值和字段信息
	 * 
	 * @param namespace  命名空间
	 * @param dataSetName 数据集名称
	 * @param bytes  序列化字节数组
	 * @param fieldName 字段名称
	 * @return ProtoBufReturn 封装字段的值和字段信息
	 */
	/*
	public static ProtobufPair getMessageValue(String namespace, String dataSetName,byte[] bytes,String fieldName){
		GeneratedMessage protObject = makeProtobufObjectByBytes(namespace, dataSetName,bytes);
		return getMessageValue(protObject,fieldName);
	}
	*/
	
	/**
	 * 获取protobuf对象中field字段的值和字段信息
	 * @param protObject Protobuf对象
	 * @param fieldName  字段名称 
	 * @return ProtoBufReturn 封装字段的值和字段信息
	 */
	/*
	public static ProtobufPair getMessageValue(GeneratedMessage protObject ,String fieldName){
		Descriptor  descriptor = protObject.getDescriptorForType();
		FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);	
		if(null==fieldDescriptor){
			return null;
		}
		Object value = protObject.getField(fieldDescriptor);	
		ProtobufPair result = new ProtobufPair(fieldDescriptor,value);
		return result;
	}
	*/

	/**
	 * 获取protobuf对象中field字段的值
	 * @param protObject Protobuf对象
	 * @param fieldName  字段名称 
	 * @return String 
	 */
	public static String getMessageStringValue(GeneratedMessage protObject ,String fieldName){
		Descriptor  descriptor = protObject.getDescriptorForType();
		FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);	
		if(null==fieldDescriptor){
			log.error("The proto Object not contain the field "+fieldName+"\n gm:\n "+protObject);
			return null;
		}
		Object value = protObject.getField(fieldDescriptor);	
		return value.toString();
	}
	
	/**
	 * 获取protobuf对象中field字段的值,字段不存在时,不输出log
	 * @param protObject Protobuf对象
	 * @param fieldName  字段名称 
	 * @return String 
	 */
	public static String getMessageStringValueNoLog(GeneratedMessage protObject ,String fieldName){
		Descriptor  descriptor = protObject.getDescriptorForType();
		FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);	
		if(null==fieldDescriptor){
//			log.error("The proto Object not contain the field "+fieldName+"\n gm:\n "+protObject);
			return null;
		}
		Object value = protObject.getField(fieldDescriptor);	
		return value.toString();
	}
	
	/**
	 * protobuf对象转换成字符串(String)
	 * Field为元数据中的field
	 * @return String
	 */
	/*
	public static String gmToString(GeneratedMessage protObject , List<com.run.ayena.mgr.metadata.Field> list){
		Descriptor  descriptor = protObject.getDescriptorForType();
		StringBuffer sb = new StringBuffer();
		for(com.run.ayena.mgr.metadata.Field field : list){
			FieldDescriptor fieldDescriptor = descriptor.findFieldByName(field.getAliasEID());	
			Object value = protObject.getField(fieldDescriptor);	
			sb.append(value.toString() + Constants.TAB);
		}
		return sb.substring(0,sb.length()-1);
	}
	*/

	/**
	 * protobuf对象转换成字符串(String)
	 * @param protObject对象
	 * @param list 元数据字段外码 列表
	 * @param ads  元数据配置解析和元数据
	 * @param AliasDSID 数据集内码
	 * @return 
	 */
	/*
	public static String gmToString(GeneratedMessage protObject, String[] list, AyenaDataSetFile ads, String AliasDSID){
		Descriptor  descriptor = protObject.getDescriptorForType();
		StringBuffer sb = new StringBuffer();
		for(String field : list){
			String aliasfield = null;
			try {
				aliasfield = ads.getFiledAliasEIDByEid(AliasDSID, field);//.getStoreFieldId(DSID, field);
			} catch (IOException e) {
				e.printStackTrace();
			}
			FieldDescriptor fieldDescriptor = descriptor.findFieldByName(aliasfield);	
			//protObject对象中aliasfield字段"值"为空时，fieldDescriptor==null
			if(null == fieldDescriptor){
				sb.append(Constants.TAB); 
			}else{
				Object value = protObject.getField(fieldDescriptor);
				sb.append(value.toString()+Constants.TAB);
			}
		}
		return sb.substring(0,sb.length()-1);
	}
	*/
	
	/**
	 * protobuf对象转换成字符串(String)
	 * @param protObject对象
	 * @param aliaslist 元数据字段内码 列表 或者 固定字符串,例如：RB050014,RB050016,abc
	 * @return 
	 */
	public static String gmToString(GeneratedMessage protObject, String[] aliaslist){
		Descriptor  descriptor = protObject.getDescriptorForType();
		StringBuffer sb = new StringBuffer();
		boolean bExistData = false;
		for(String aliasfield : aliaslist){
	
			FieldDescriptor fieldDescriptor = descriptor.findFieldByName(aliasfield);	
			//protObject对象中aliasfield字段"值"为空时，fieldDescriptor==null
			if(null == fieldDescriptor){
				sb.append(aliasfield + Constants.TAB); 
			}else{
				Object value = protObject.getField(fieldDescriptor);
				if(value != null && !value.toString().isEmpty())
				{
					bExistData = true;
					sb.append(value.toString()+Constants.TAB);
				}
				else
				{
					sb.append(Constants.TAB);
				}
				
			}

		}
		if(bExistData){
			return sb.substring(0,sb.length()-1);
		}
		else
		{
			return "";
		}
	}
	
	/**
	 * 通过bilder，获取protobuf对象中field字段的值和字段信息
	 * @param builder  builder对象
	 * @param fieldName 字段名称
	 * @return 封装字段的值和字段信息
	 */
	/*
	public static ProtobufPair getMessageValue(Message.Builder builder ,String fieldName){
		Descriptor  descriptor = builder.getDescriptorForType();
		FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);
		if(null==fieldDescriptor){
			return null;
		}
		Object value = builder.getField(fieldDescriptor);	
		ProtobufPair result = new ProtobufPair(fieldDescriptor,value);
		return result;
	}
	*/
	
	/**
	 * 通过builder，获取protobuf对象中field字段的值和字段信息，如果为空则使用默认值
	 * @param builder  builder对象
	 * @param fieldName 字段名称
	 * @param defaultValue 默认值，从策略文件或元数据配置文件中获取
	 * @return 封装字段的值和字段信息
	 */
	/*
	public static ProtobufPair getMessageValueUseDefault(Message.Builder builder ,String fieldName, String defaultValue){
		Descriptor  descriptor = builder.getDescriptorForType();
		FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);
		if(null==fieldDescriptor){
			return null;
		}
		Object value = builder.getField(fieldDescriptor);		
		if(value==null || value.toString().isEmpty()){
			value=defaultValue;
		}
		ProtobufPair result = new ProtobufPair(fieldDescriptor,value);
		return result;
	}
	*/
	
	/**
	 * 设置protobuf对象的字段值
	 * @param protObject Protobuf对象
	 * @param fieldName 字段名称 
	 * @param value 字段值
	 * @return 
	 */
	public static GeneratedMessage setMessageValue(GeneratedMessage protObject ,String fieldName,Object value){
		Descriptor  descriptor = protObject.getDescriptorForType();
		FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);
//		value = convertJavaType(fieldDescriptor,value);				
		Builder builder = protObject.toBuilder();
		try {
			if("INT".equals(fieldDescriptor.getJavaType().name().toString())){
				builder.setField(fieldDescriptor, Integer.parseInt(value.toString()));				
			}else if("LONG".equals(fieldDescriptor.getJavaType().name().toString())){
				builder.setField(fieldDescriptor, Long.parseLong(value.toString()));
			}else if("FLOAT".equals(fieldDescriptor.getJavaType().name().toString())){
				builder.setField(fieldDescriptor, Float.parseFloat(value.toString()));				
			}else if("DOUBLE".equals(fieldDescriptor.getJavaType().name().toString())){
				builder.setField(fieldDescriptor, Double.parseDouble(value.toString()));				
			}else {
				builder.setField(fieldDescriptor, value);				
			}
			
			protObject = (GeneratedMessage) builder.build();
		} catch (IllegalArgumentException  e) {
			e.printStackTrace();			
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			builder=null;
			fieldDescriptor=null;
			descriptor=null;
		}
		//log.debug(protObject.toString());
		return protObject;
	}
	/**
	 * 为builder 设置值
	 * @param builder  
	 * @param fieldName
	 * @param value
	 */
	/*
	public static void setMessageValue(Message.Builder builder,String fieldName,Object value){
		//try {
		long rulestart = System.currentTimeMillis(); 
			Descriptor  descriptor = builder.getDescriptorForType();
			
			FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);	
			value = convertJavaType(fieldDescriptor,value);
			builder.setField(fieldDescriptor, value);
			
		ObjectDistillPfMetric.setmessagetime = ObjectDistillPfMetric.setmessagetime + (System.currentTimeMillis() - rulestart);
		ObjectDistillPfMetric.setmessagecount ++;	
		//} catch (Exception e) {
		//		e.printStackTrace();
		//}
	}
	*/
	
	/**
	 * Protobuf对象构造器	，构造空对象
	 * 
	 * @param namespace 命名空间
	 * @param dataSetName 数据集名称
	 * @return GeneratedMessage 空的Protobuf对象
	 */
	@Deprecated
	public static GeneratedMessage makeProtobufObjectByName(String namespace, String dataSetName){		
		try {			
			Class<?> clazz = Class.forName(getClassName(namespace, dataSetName)+"$Builder");		
			Method method = clazz.getDeclaredMethod("create");			
			method.setAccessible(true);				
			@SuppressWarnings("rawtypes")
			GeneratedMessage.Builder builder = (GeneratedMessage.Builder)method.invoke(null);
			GeneratedMessage gm = (GeneratedMessage)builder.build();
			return gm;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * protobuf builder构造器，构造空的builder对象
	 * @param namespace
	 * @param DSID 内码
	 * @return
	 */
	public static Message.Builder makeProtbufBuilderByName(String namespace, String DSID){
		try {
			String key = namespace + Constants.SPECIALCHAR + DSID;		
			Method method = null;
			if(methodMap.containsKey(key)){
				method = methodMap.get(key);
			}else{
				Class<?> clazz = Class.forName(getClassName(namespace, DSID)+"$Builder");
				method = clazz.getDeclaredMethod("create");
				method.setAccessible(true);
				methodMap.put(key, method);
			}
			Message.Builder builder = (Message.Builder)method.invoke(null);
			return builder;
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}			
		return null;
	}
	
	/**
	 * Protobuf对象构造器	，构造‘带值’对象
	 * @param namespace 命名空间
	 * @param dataSetName 数据集名称
	 * @param bytes  对象的序列化字节数组
	 * @return GeneratedMessage  带值的Protobuf对象
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static GeneratedMessage makeProtobufObjectByBytes(String namespace, String dataSetName, byte[] bytes) {
		try {
			String key = namespace + Constants.SPECIALCHAR + dataSetName;		
			Method method = null;
			if(parserMethodMap.containsKey(key)){
				method = parserMethodMap.get(key);
			}else{
				Class clazz = Class.forName(getClassName(namespace, dataSetName));
				method = clazz.getDeclaredMethod("parseFrom", byte[].class);
				method.setAccessible(true);
				parserMethodMap.put(key, method);
			}
			return (GeneratedMessage) method.invoke(null, bytes);
		} catch (Exception e) {
			log.error("构造Protobuf对象错误:"+e.getMessage());
			e.printStackTrace();
		} 
		return null;
	}
	/**
	 * Protobuf对象构造器	，构造‘带值’对象
	 * @param namespace 命名空间
	 * @param dataSetName 数据集名称
	 * @param bytes 对象的序列化字节数组
	 * @param begin 字节开始位置
	 * @param end	数组结束位置
	 * @return  带值的Protobuf对象 
	 */
	public static GeneratedMessage makeProtobufObjectByBytes(String namespace, String dataSetName, byte[] bytes,int begin,int end) {
		try {
			String key = namespace + Constants.SPECIALCHAR + dataSetName;	
			Parser<?> parser = null;
			if(parserMap.containsKey(key)){
				parser = parserMap.get(key);
			}else{
				Class<?> clazz = Class.forName(getClassName(namespace, dataSetName));
				Field  field= clazz.getDeclaredField("PARSER");
				field.setAccessible(true);
				parser = (Parser<?>) field.get(null);
				parserMap.put(key, parser);
			}

			return (GeneratedMessage)parser.parseFrom(bytes, begin, end);
			
		} catch(InvalidProtocolBufferException e){
			log.error("Protobuf反序列化错误:"+e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {			
			log.error("构造Protobuf对象错误:"+e.getMessage());
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Protobuf对象构造器	，构造‘带值’对象
	 * 
	 * @param key 
	 * @param bytes  对象的序列化字节数组
	 * @return GeneratedMessage  带值的Protobuf对象
	 */
	public static GeneratedMessage makeProtobufObjectByBytes(String key, byte[] bytes) {
		String[] strs = key.split(",");
		return makeProtobufObjectByBytes(strs[0],strs[1],bytes);
	}
	
	/**
	 *  通过map构建protobuf对象,key为fieldName,value不能是repeated
	 * @param namespace 命名空间
	 * @param dataSetName 数据集名称
	 * @param map  保持对象数据信息的map
	 * @return 返回构建好的protobuf对象
	 */
	public static GeneratedMessage makeProtobufObjectByMap(String namespace, String dataSetName, Map<String, String> map) {				
			Message.Builder builder = makeProtbufBuilderByName(namespace,dataSetName);			
			Descriptor descriptor = builder.getDescriptorForType();
			Iterator<String> it = map.keySet().iterator();
			while(it.hasNext()){
				String fieldName = it.next();
				String value = map.get(fieldName);
				if(null==value){
					value = "";
				}				
				FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);
				//统计字段不存在,则丢弃
				if(fieldDescriptor == null){
					log.debug("ProtoBufTools.makeProtobufObjectByMap():field is not exist:namespace="+namespace+";dataSetName="+dataSetName+";fieldName="+fieldName);					
				}
				else{
					builder.setField(fieldDescriptor, convertJavaType(fieldDescriptor, value));
				}
			}
			return (GeneratedMessage)builder.build();		
	}
	
	public static GeneratedMessage makeStringProtobufObjectByMap(Map<String, String> map) {
//		List<String> hiveSchema = getHiveSchema(namespace, dataSetName); // 获取hive的schema
		
		Message.Builder builder = (Message.Builder) new Object();			
		Descriptor descriptor = builder.getDescriptorForType();
		Iterator<String> it = map.keySet().iterator();
		while(it.hasNext()){
			String fieldName = it.next();
			String value = map.get(fieldName);
			if(null==value){
				value = "";
			}				
			FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);
			//统计字段不存在,则丢弃
			if(fieldDescriptor == null){
				log.debug("ProtoBufTools.makeProtobufObjectByMap():field is not exist: fieldName=" + fieldName);					
			}
			else{
				builder.setField(fieldDescriptor, convertJavaType(fieldDescriptor, value));
			}
		}
		return (GeneratedMessage) builder.build();		
}
	
	/**
	 * 通过map构建protobuf对象bilder ,key为fieldName,value不能是repeated
	 * @param namespace
	 * @param DSID 内码
	 * @param map
	 * @return 返回构造好的builder对象
	 */
	public static Message.Builder makeProtobufBuilderByMap(String namespace,String DSID, Map<String, String> map){
		Message.Builder builder = makeProtbufBuilderByName(namespace,DSID);
		Descriptor descriptor = builder.getDescriptorForType();
		Iterator<String> it = map.keySet().iterator();
		while(it.hasNext()){
			String fieldName = it.next();
			String value = map.get(fieldName);
			if(null==value){
				value = "";
			}				
			FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldName);
			//统计字段不存在,则丢弃
			if(fieldDescriptor == null){
				log.debug("ProtoBufTools.makeProtobufBuilderByMap():field is not exist:namespace="+namespace+";DSID="+DSID+";fieldName="+fieldName);
			} else {
				builder.setField(fieldDescriptor, convertJavaType(fieldDescriptor, value));
			}
		}
		return builder;
	}
	
	/**
	 * 深克隆protobuf对象
	 * @param protObject 被克隆的protobuf
	 * @return 返回克隆后的protobuf对象
	 */
	public static GeneratedMessage cloneProtobufObject(GeneratedMessage protObject){
		return (GeneratedMessage) protObject.toBuilder().build();
	}
	
	/**
	 * protobuf对象中的类型转换
	 * @param fieldDescriptor 属性描述对象
	 * @param value 属性值
	 * @return  返回转换后的java类型对象
	 */
	public static Object convertJavaType(FieldDescriptor fieldDescriptor,Object value){
		FieldDescriptor.JavaType javaType = fieldDescriptor.getJavaType();
		switch (javaType) {
			case DOUBLE:
				return Double.valueOf(String.valueOf(value));
			case FLOAT:
				return Float.valueOf(String.valueOf(value));
			case LONG:
				return Long.valueOf(String.valueOf(value));
			case INT:
				return Integer.valueOf(String.valueOf(value));
			case BOOLEAN:
				return Boolean.valueOf(String.valueOf(value));
			case STRING:
				return String.valueOf(value);
			default:
				return value;
		}
	}
	
	/**
	 * protobuf对象字段类型转换
	 * @param desField 目标字段类型信息
	 * @param srcField 源字段类型信息
	 * @param value    源字段的value
	 * @return  返回转换后的value
	 * @throws IOException 
	 * @throws ParseException
	 */
	/*
	public static Object convertJavaType(FieldDescriptor desField,FieldDescriptor srcField,Object value,String namespace, String DSID) throws IOException, ParseException{
		FieldDescriptor.JavaType desType = desField.getJavaType();
		FieldDescriptor.JavaType srcType = srcField.getJavaType();
		
		try{
		if(!srcType.equals(desType)){
			//如果是从string提取到long类型中，目前有两种，一种string-->number，一种string-->timestamp
			if(srcType.equals(desType.STRING) && desType.equals(desType.LONG)){
				com.run.ayena.mgr.metadata.Field field = AyenaDataSetFile.getInstance(namespace).getDatasetFieldByAAliasEID(DSID, desField.getName());
				//如果目标是timestamp，则说明源是时间的字符串格式（来源也有两种，一种是时间戳格式的字符串，另一种是20160202格式的时间）
				if(field.getValueType().equalsIgnoreCase("timestamp")){
					//如果来源是8位长度的字符串，则说明时间格式为20160202
					if(value.toString().length() == 8){
						//将源日志中的字段格式化成yyyyMMdd格式的时间，并获取对应的时间戳（需要/1000来获取到秒级）
						return sdf.parse(value.toString()).getTime()/1000;
					}else{
						//如果来源不是8位长度的字符串，则说明时间格式就是原始的时间戳
						return convertJavaType(desField,value);
					}
					
				}else{
					return convertJavaType(desField,value);
				}
			}else{
				return convertJavaType(desField,value);				
			}
		}
		}catch(Exception e){
			log.error("convertJavaType:srcField=" + srcField.getName()
					+ ";desField=" + desField.getName() + ";srcType=" + srcType
					+ ";desType=" + desType);
			throw e;
		}
		return value;
	}
	*/
	
	/**
	 * 获取protobuf对象里所有的字段名称
	 * @param protObject
	 * @return 返回字段名称列表
	 */
	public static List<String> getProtbufObjectAllFields(GeneratedMessage protObject){
		List<String> list = new ArrayList<String>();
		List<FieldDescriptor> fields = getProtbufObjectAllFieldDescriptor(protObject);
		for(FieldDescriptor field : fields){
			list.add(field.getName());
		}
		return list;
	}
	
	/**
	 * 获取protobuf对象里所有的字段FieldDescriptor
	 * @param protObject
	 * @return 返回字段FieldDescriptor列表
	 */
	public static List<FieldDescriptor> getProtbufObjectAllFieldDescriptor(GeneratedMessage protObject){
		Descriptor descriptor = protObject.getDescriptorForType();
		List<FieldDescriptor> fields = descriptor.getFields();
		return fields;
	}
	
	
	/**
	 * 获取builder对象里所有的字段名称
	 * @param protObject
	 * @return 返回字段名称列表
	 */
	public static List<String> getBuilderObjectAllFields(Message.Builder protObject){
		List<String> list = new ArrayList<String>();
		List<FieldDescriptor> fields = getBuilderObjectAllFieldDescriptor(protObject);
		for(FieldDescriptor field : fields){
			list.add(field.getName());
		}
		return list;
	}
	
	/**
	 * 获取builder对象里所有的字段FieldDescriptor
	 * @param protObject
	 * @return 返回字段FieldDescriptor列表
	 */
	public static List<FieldDescriptor> getBuilderObjectAllFieldDescriptor(Message.Builder protObject){
		Descriptor descriptor = protObject.getDescriptorForType();
		List<FieldDescriptor> fields = descriptor.getFields();
		return fields;
	}
	
	/**
	 * 结构化去重中，新数据和老数据的合并
	 * 主要目的：将newMap中的数据put到oldMap中,对oldMap进行覆盖操作
	 * @param newMap 新数据Map
	 * @param oldMap 老数据Map Map为不可修改Map
	 * @return key为Protobuf对象属性描述和属性值
	 */
	public static Map<FieldDescriptor,Object> structDataMerge(Map<FieldDescriptor,Object> newMap,
			Map<FieldDescriptor,Object> oldMap){
		Map<FieldDescriptor,Object> mergeMap = new HashMap<FieldDescriptor,Object>(oldMap);
		if(!newMap.isEmpty()){
			Iterator<FieldDescriptor> it = newMap.keySet().iterator();
			while(it.hasNext()){
				FieldDescriptor key = it.next();
				Object value = newMap.get(key);
				if(value!=null && !String.valueOf(value).isEmpty()){
					mergeMap.put(key, newMap.get(key));
				}
			}
		}
		oldMap=null;
		return mergeMap;
	}
		
	/**
	 * Map<FieldDescriptor,Object>转换成protobuf对象
	 * @param namespace 命名空间
	 * @param DSID 数据集内码
	 * @param map key为protobuf对象属性描述，value为属性值
	 * @return protobuf对象
	 */
	public static GeneratedMessage mapToProtobuf(String namespace, String DSID, Map<FieldDescriptor,Object> map){
		Message.Builder builder = makeProtbufBuilderByName(namespace,DSID);
		Iterator<FieldDescriptor> it = map.keySet().iterator();
		while(it.hasNext()){
			FieldDescriptor fieldDes = it.next();
			builder.setField(fieldDes, map.get(fieldDes));
		}
		return (GeneratedMessage) builder.build();
	}
	/**
	 * 获取protobuf对象中的字段描述
	 * @param namespace
	 * @param DSID
	 * @param fieldCode 内码
	 * @return
	 * @throws IOException 
	 */
	public static FieldDescriptor getFieldDescriptorByname(String namespace, String DSID, String fieldCode){
		Message.Builder builder = makeProtbufBuilderByName(namespace,DSID);
		Descriptor descriptor = builder.getDescriptorForType();
		FieldDescriptor fieldDescriptor = descriptor.findFieldByName(fieldCode);			
		return fieldDescriptor;
	}
	
	
	/**
	 * 根据namespace和dataSetName获取类的详细路径
	 * @param namespace 命名空间
	 * @param dataSetName 数据集名称
	 * @return  String 返回类的详细路径
	 */
	private static String getClassName(String namespace, String dataSetName) {
		// patch2、patch3使用下行代码
//		 return namespace + "."+dataSetName + "_$" + dataSetName;

		// patch4开始，使用下行代码
		return "com.run.ayena.dataset." + dataSetName + "_$" + dataSetName;
	}
	
	public static ObjectBase setMessageValue(ObjectBase ob ,String fieldName,String value){
		ObjectBase.Builder obb = ob.toBuilder();
		for(ObjectBaseAttr.Builder obab :obb.getPropsBuilderList()){
			if(obab.getCode().equalsIgnoreCase(fieldName)){
				obab.setCode(fieldName);
				ObjectCounter.Builder ocb = obab.getValuesBuilderList().get(0);
				ocb.setValue(value);
			}
		}
		return obb.build();
	}
	
	// 获取ObjectInfo中指定属性字段的值的列表
		public static List<String> getMessagePropsValueFromInfo(ObjectInfo oinfo,
				String fidldName) {
			List<ObjectInfoAttr> propsList = oinfo.getPropsList();
			for (ObjectInfoAttr oia : propsList) {
				if (oia.getCode().equalsIgnoreCase(fidldName)) {
					return oia.getValuesList();
				}
			}
			return null;
		}

		// 获取ObjectInfo中指定主键字段的值
		public static String getMessageIDsValueFromInfo(ObjectInfo oinfo,
				String fidldName) {
			List<ObjectAttr> idsList = oinfo.getIdsList();
			for (ObjectAttr oa : idsList) {
				if (oa.getCode().equalsIgnoreCase(fidldName)) {
					return oa.getValue();
				}
			}
			return null;
		}
		
		// 获取ObjectBase中指定属性字段的值的列表
		public static List<String> getMessagePropsValueFromBase(ObjectBase oinfo,
				String fidldName) {
			List<ObjectBaseAttr> propsList = oinfo.getPropsList();
			List<String> list = new ArrayList<String>();
//			for (ObjectBaseAttr oia : propsList) {
			for (int i = 0; i < propsList.size(); i ++) {
				if (propsList.get(i).getCode().equalsIgnoreCase(fidldName)) {
					for(ObjectCounter values:propsList.get(i).getValuesList()){
						if(values.getValue() != null  && !values.getValue().isEmpty()){
							list.add(values.getValue());
						}
					}
//					ObjectCounter values = propsList.get(i).getValues(i);
//					list.add(values.getValue());
				}
			}
			return list;
		}
		
		public static List<String> getMessagePropsValueFromBase(com.jy.pb.ObjectData.ObjectBase.Builder obBuilder,
				String fidldName) {
			List<ObjectBaseAttr> propsList = obBuilder.getPropsList();
			List<String> list = new ArrayList<String>();
//			for (ObjectBaseAttr oia : propsList) {
			for (int i = 0; i < propsList.size(); i ++) {
				if (propsList.get(i).getCode().equalsIgnoreCase(fidldName)) {
					for(ObjectCounter values:propsList.get(i).getValuesList()){
						if(values.getValue() != null  && !values.getValue().isEmpty()){
							list.add(values.getValue());
						}
					}
//					ObjectCounter values = propsList.get(i).getValues(i);
//					list.add(values.getValue());
				}
			}
			return list;
		}
		
		public static ObjectBaseAttr.Builder getPropsBuilderFromBase(com.jy.pb.ObjectData.ObjectBase.Builder obBuilder,
				String fidldName) {
			List<com.jy.pb.ObjectData.ObjectBaseAttr.Builder> propsList = obBuilder.getPropsBuilderList();
			List<String> list = new ArrayList<String>();
//			for (ObjectBaseAttr oia : propsList) {
			for (int i = 0; i < propsList.size(); i ++) {
				if (propsList.get(i).getCode().equalsIgnoreCase(fidldName)) {
					return propsList.get(i);
//					ObjectCounter values = propsList.get(i).getValues(i);
//					list.add(values.getValue());
				}
			}
			return null;
		}

		// 获取ObjectBase中指定主键字段的值
		public static String getMessageIDsValueFromBase(ObjectBase oinfo,
				String fidldName) {
			List<ObjectAttr> idsList = oinfo.getIdsList();
			for (ObjectAttr oa : idsList) {
				if (oa.getCode().equalsIgnoreCase(fidldName)) {
					return oa.getValue();
				}
			}
			return null;
		}
		
		public static String getMessageIDsValueFromBase(com.jy.pb.ObjectData.ObjectBase.Builder obBuilder,
				String fidldName) {
			List<ObjectAttr> idsList = obBuilder.getIdsList();
			for (ObjectAttr oa : idsList) {
				if (oa.getCode().equalsIgnoreCase(fidldName)) {
					return oa.getValue();
				}
			}
			return null;
		}
		
		// 获取ObjectBase中指定dims字段的值
		public static String getMessageDimsValueFromBase(ObjectBase oinfo,
				String fidldName) {
			List<ObjectAttr> dimsList = oinfo.getDimsList();
			for (ObjectAttr oa : dimsList) {
				if (oa.getCode().equalsIgnoreCase(fidldName)) {
					return oa.getValue();
				}
			}
			return null;
		}
		
		
		public static String getMessageDimsValueFromBase(com.jy.pb.ObjectData.ObjectBase.Builder obBuilder,
				String fidldName) {
			List<ObjectAttr> dimsList = obBuilder.getDimsList();
			for (ObjectAttr oa : dimsList) {
				if (oa.getCode().equalsIgnoreCase(fidldName)) {
					return oa.getValue();
				}
			}
			return null;
		}
		
		// 获取ObjectBase中字段的值,如果字段值为空或者空字符串，则返回空的list
		public static List<String> getMessageValueFromBase(ObjectBase oinfo, String fieldName) {
			List<String> list = new ArrayList<String>();
			String strRet = "";
			strRet = getMessageIDsValueFromBase(oinfo, fieldName);
			if (strRet == null) {
				strRet = getMessageDimsValueFromBase(oinfo, fieldName);
			} 
			
			if(strRet == null){
				list = getMessagePropsValueFromBase(oinfo, fieldName);	
			} 
			else if(!strRet.isEmpty()){
				list.add(strRet);
			}			
			return list;
		}
		
		public static List<String> getMessageValueFromBase(com.jy.pb.ObjectData.ObjectBase.Builder obBuilder,
				String fieldName) {
			List<String> list = new ArrayList<String>();
			String strRet = "";
			strRet = getMessageIDsValueFromBase(obBuilder, fieldName);
			if (strRet == null) {
				strRet = getMessageDimsValueFromBase(obBuilder, fieldName);
			} 
			
			if(strRet == null){
				list = getMessagePropsValueFromBase(obBuilder, fieldName);	
			} 
			else if(!strRet.isEmpty()){
				list.add(strRet);
			}			
			return list;
		}
		
		/**
		 * 方法描述:设置ObjectBase中ids字段中指定字段的值
		 * 
		 * @param obBuilder ob的builder
		 * @param fieldName 字段名称
		 * @param value 需要设置的值
		 * @return
		 */
		public static com.jy.pb.ObjectData.ObjectBase.Builder setObjectBaseIdsValue(com.jy.pb.ObjectData.ObjectBase.Builder obBuilder ,String fieldName,String value){
			
			List<com.jy.pb.ObjectData.ObjectAttr.Builder> idsBuilderList = obBuilder.getIdsBuilderList();
			
			for(com.jy.pb.ObjectData.ObjectAttr.Builder obab : idsBuilderList){
				if(obab.getCode().equalsIgnoreCase(fieldName)){
					obab.setValue(value);
					break;
				}
			}
			return obBuilder;
		}
}
