package com.bbd.FlinkTest.utils;

/**
 * @Author: maketubu
 * @Date: 2019/10/30 22:59
 */

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Author: maketubu
 * @Date: 2019/10/30 16:04
 */
public class PropertiesTool {
    public static String getproperties(String name, String filename){
        Properties properties = new Properties();
        InputStream istream = PropertiesTool.class.getResourceAsStream("/"+filename);
        String values = null;
        try{
            properties.load(istream);
            values = properties.getProperty(name);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{istream.close();}catch (Exception e){e.printStackTrace();}
        }
        return values;
    }

    public static Map<String, String> getEsproperties(String fileName) {
        Properties paramProp = new Properties();
        InputStream inputStream = PropertiesTool.class.getResourceAsStream("/" + fileName);
        Map<String, String> map = new HashMap<String, String>();
        try{
            paramProp.load(inputStream);
            for(Map.Entry<Object, Object> entry : paramProp.entrySet()){
                String key = entry.getKey().toString();
                String value = entry.getValue().toString();
                map.put(key, value);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{inputStream.close();} catch (Exception e){};
        }
        return map;
    }

    public static void main(String[] args) {
        System.out.print(PropertiesTool.getEsproperties("es.properties"));
//        PropertiesTool.getEsproperties("es.properties");
    }
}


