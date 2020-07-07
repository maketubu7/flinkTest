package com.bbd.FlinkTest.flinkSource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Copyright@paidaxing
 * Author: paidaxing
 * Date:2020/7/3
 * Description:
 */
public class TableSqlApp {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        tablesqlFunction(env);
    }


    public static void tablesqlFunction(ExecutionEnvironment env) throws Exception{
        BatchTableEnvironment tenv = TableEnvironment.getTableEnvironment(env);
        String filePath = "E:\\idea_workspace\\FlinkTest\\output\\scala\\astext_data.txt";
        // 这种方法不行，会有 找不到该字段（id）的异常
        /*
         *Caused by: org.apache.calcite.sql.validate.SqlValidatorException: Column 'id' not found in any tabl
         * */
        DataSet<ids> dataset  = env.readTextFile(filePath).map(new MapFunction<String, ids>() {
            @Override
            public ids map(String value) throws Exception {
                return new ids(value);
            }
        });
//        DataSet<ids> dataset  = env.readCsvFile(filePath).ignoreFirstLine().pojoType(ids.class,"id");
        dataset.print();
        Table ids_table = tenv.fromDataSet(dataset);
        tenv.registerTable("ids_table",ids_table);
        Table ids_res = tenv.sqlQuery("select id from ids_table");
        tenv.toDataSet(ids_res, Row.class).print();
    }

    public static class ids {
        public String id;

        public ids(String id) {
            this.id = id;
        }
//
        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }

}
