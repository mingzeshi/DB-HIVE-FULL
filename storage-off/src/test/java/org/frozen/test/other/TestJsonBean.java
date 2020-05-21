package org.frozen.test.other;

import org.frozen.util.HadoopTool;
import net.sf.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestJsonBean {

    public static  void main(String[] args) {

//        try {
//            FileSystem fs = FileSystem.get(new URI("hdfs://linux01:8020"), new Configuration(), "hadoop");
//
//            List<String> textList = HadoopTool.readTextFromFile(fs, new Path("/jlcstore/datamerge_schema/schema.json"));
//
//            Map<String, SchemaBean> schemaBeanMap = new HashMap<String, SchemaBean>();
//
//            for (String line : textList) {
//                SchemaBean schemaBean = (SchemaBean) JSONObject.toBean(JSONObject.fromObject(line), SchemaBean.class);
//                schemaBeanMap.put(schemaBean.getDb() + "." + schemaBean.getTable(), schemaBean);
//            }
//
//            for(String key : schemaBeanMap.keySet()) {
//                SchemaBean schemaBean = schemaBeanMap.get(key);
//
//                System.out.println(schemaBean);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}
