package com.sql.parse.dao;

import com.sql.parse.util.MysqlUtil;
import java.sql.SQLException;

public class DataWarehouseDao {
    MysqlUtil dbUtil = new MysqlUtil("WAREHOUSE");

    public void insertColumnDependencies(String columnName, String dependencyColumn) throws SQLException {
        String deleteSql = "delete from metadata_column_dependencies_mapping where column_name = '" + columnName + "'";
        dbUtil.doDelete(deleteSql);

        String insertSql = "insert into metadata_column_dependencies_mapping(column_name, dependency_column_name) values('"
                + columnName + "','" + dependencyColumn + "')";
        dbUtil.doInsert(insertSql);
    }
}
