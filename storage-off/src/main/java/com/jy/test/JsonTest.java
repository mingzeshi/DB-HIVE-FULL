package com.jy.test;

import com.google.gson.JsonObject;
import com.jy.parse.BinLogJsonParse;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;

public class JsonTest {

    public static void main(String[] args) {

        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;


        try {
            File idDir = new File("C:/Users/Administrator/Desktop/文件/20180817/binlog.txt");

            fis = new FileInputStream(idDir);
            isr = new InputStreamReader(fis, "UTF-8");
            br = new BufferedReader(isr);

            String str = "";

            while ((str = br.readLine()) != null) {
                /*
                JSONObject jsonObject = JSONObject.fromObject(str);
                JSONObject jsonObjectHead = jsonObject.getJSONObject("head");

                String type = jsonObjectHead.getString("type");
                String db = jsonObjectHead.getString("db");
                String table = jsonObjectHead.getString("table");

                System.out.println(type + "-" + db + "-" + table);
                */
                /*
                Map<String, String> paramMap = BinLogJsonParse.parseColumnNameAndValue(jsonObject, "after", "id", "applyTime");

                for(String key : paramMap.keySet()) {
                    System.out.println(key + "-" + paramMap.get(key));
                }
                */

                JSONObject jsonObject = JSONObject.fromObject(str);

                Iterator<JSONObject> jsonArray = jsonObject.getJSONArray("after").iterator();
                List<String> columnList = new ArrayList<String>();

                while(jsonArray.hasNext()) {
                    columnList.add(jsonArray.next().getString("name"));
                }

                for(String column : columnList) {
                    System.out.println(column);
                }
            }

        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
