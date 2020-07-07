package com.bbd.FlinkTest.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.bbd.FlinkTest.utils.PropertiesTool;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Copyright@paidaxing
 * Author: paidaxing
 * Date:2020/7/3
 * Description:
 */
public class DButiles {

    public static Connection conn = null;
    public static void getConnection() throws Exception{
        String user = PropertiesTool.getproperties("user","mysql.properties");
        String passwd = PropertiesTool.getproperties("passwd","mysql.properties");
        String jdbc = PropertiesTool.getproperties("jdbc","mysql.properties");
        conn = DriverManager.getConnection(jdbc,user,passwd);
        conn.setAutoCommit(true);
    }

    public static void insertinsertIntoStudetn(Tuple2<Integer,String> value) throws Exception{
        getConnection();
        String insert_sql = String.format("INSERT into student(id, name) " +
                "VALUES (%d, '%s') ON DUPLICATE KEY UPDATE id = %d",value.f0, value.f1,value.f0);
        PreparedStatement ps = conn.prepareStatement(insert_sql);
        ps.execute();
    }

    public static void returnConnection(){
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }}
