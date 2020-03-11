package com.sql.parse.dao;

import com.sql.parse.util.MysqlUtil;
import java.sql.SQLException;

public class DataWarehouseDao {
    MysqlUtil dbUtil = new MysqlUtil("WAREHOUSE");

    public void deleteColumnDependencies(String tableName) {
        try {
            String deleteSql = "delete from metadata_column_dependencies_mapping where table_name = '" + tableName + "'";
            dbUtil.doDelete(deleteSql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void insertColumnDependencies(String tableName, String columnName, String dependencyColumn) throws SQLException {
        String insertSql = "insert into metadata_column_dependencies_mapping(table_name, column_name, dependency_column_name) " +
                "values('" + tableName + "','" +  columnName + "','" + dependencyColumn + "')";
        dbUtil.doInsert(insertSql);
    }


    public void deleteTableDependencies(String tableName) {
        try {
            String deleteSql = "delete from metadata_table_dependencies_mapping where table_name = '" + tableName + "'";
            dbUtil.doDelete(deleteSql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void insertTableDependencies(String tableName, String dependencyTable) throws SQLException {
        String insertSql = "insert into metadata_table_dependencies_mapping(table_name, dependency_table_name) " +
                "values('" + tableName + "','" +  dependencyTable + "')";
        dbUtil.doInsert(insertSql);
    }

    public void deleteJoinRelation(String filePath) {
        try {
            String deleteSql = "delete from metadata_table_join_on_relation where file_path = '" + filePath + "'";
            dbUtil.doDelete(deleteSql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void insertJoinRelation(String fullLeftTableName, String fullRightTableName,
                                   String leftColumns, String rightColumns, String filePath) throws SQLException {
        String insertSql = "insert into metadata_table_join_on_relation(left_table, right_table, left_columns, " +
                "right_columns, file_path) values('" + fullLeftTableName + "','" +  fullRightTableName + "','" +
                leftColumns + "','" + rightColumns + "','" + filePath + "')";
        dbUtil.doInsert(insertSql);
    }
}
