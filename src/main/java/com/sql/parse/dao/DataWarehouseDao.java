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
}
