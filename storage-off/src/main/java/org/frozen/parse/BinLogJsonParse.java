package org.frozen.parse;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import java.util.*;
import org.frozen.vo.Tuple;
import org.frozen.util.StringUtil;


/**
 * 用于解析binlog的json对象
 */
public class BinLogJsonParse {

    private static final Set<String> userTableSet = new HashSet<String>();
    private static final Set<String> validTableSet = new HashSet<String>();

    static{
        userTableSet.add("user_info");

        validTableSet.add("user_info");
        validTableSet.add("invest_record");//投资
        validTableSet.add("cash_record");//赎回
        validTableSet.add("recharge_record");//支付失败相关统计
        validTableSet.add("bank_card");//开户数

    }

    //解析table表名
    public static String parseTableName(JSONObject json){
        try{
            return json.getJSONObject("head").getString("table");
        }catch(Exception ex){
            return null;
        }
    }

    //解析json的表是否在集合内,在集合内说明是合法的表
    public static boolean isValidTable(JSONObject json,Set<String> validTableSet){
        try{
            String tableName = json.getJSONObject("head").getString("table");
            return validTableSet.contains(tableName);
        }catch(Exception ex){
            return false;
        }
    }

    public static boolean isValidTable(JSONObject json){
        return isValidTable(json, validTableSet);
    }

    //是否是有效的user表
    public static boolean isValidUserTable(JSONObject json){
        return isValidTable(json, userTableSet);
    }

    //返回值key是redis的key,即userid,value是userid的具体内容
    //返回每一个user对应的渠道信息
    public static Tuple<String,String> parseUserInfo(JSONObject json){
        try{
            Iterator<JSONObject> records = json.getJSONArray("after").iterator();
            Map<String,String> record = parseColumnNameAndValue(json, "after", "user_id", "userid", "channel");
            String userid = StringUtil.trim(record.get("user_id"));
            if("".equals(userid)){
                userid = StringUtil.trim(record.get("userid"));
            }
            String channel = StringUtil.trim(record.get("channel"));
            return new Tuple(userid,userid + "notag");
        } catch(Exception ex){
            return null;
        }
    }

    /**
     * @param records 在json数据中查找name属性对应的值
     * @param isUpdate true表示必须该字段的update=true,即必须要抓去修改的数据
     * @param columnName 属性name
     */
    public static Tuple<String,String> getColumnNameAndValue(JSONArray records,boolean isUpdate,String columnName){
        JSONObject record;
        int size = records.size();
        for(int i=0;i<size ;i++){
            record = records.getJSONObject(i);
            if(columnName.equalsIgnoreCase(record.getString("name"))){
                if(!isUpdate || (isUpdate && "true".equalsIgnoreCase(record.getString("update")))){
                    return new Tuple(columnName,record.getString("value"));
                }
                break;
            }
        }
        return null;
    }


    /**
     * 解析json对应的数据值,参数是要解析的列的集合
     * @param afterOrBefore after或者before
     * @param isUpdate true表示必须该字段的update=true,即必须要抓去修改的数据   用于update语法获取关键字的时候使用
     */
    public static Map<String,String> parseColumnNameAndValue(JSONObject json,boolean isUpdate,String afterOrBefore,String... columnNames){
        try{
            Map<String,String> map = new HashMap<String,String>();

            JSONArray records = json.getJSONArray(afterOrBefore);

            Tuple<String,String> tuple;

            for(String name:columnNames){
                tuple = getColumnNameAndValue(records,isUpdate,name);
                if(tuple != null){
                    map.put(tuple.getKey(),tuple.getValue());
                }
            }
            return map;
        } catch(Exception ex){
            return null;
        }
    }

    public static Map<String,String> parseColumnNameAndValue(JSONObject json,String afterOrBefore,String... columnNames){
        return parseColumnNameAndValue(json,false,afterOrBefore,columnNames);
    }

}
