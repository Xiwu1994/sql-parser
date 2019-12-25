package com.sql.parse.util;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class MysqlUtil {
    private static Logger logger = Logger.getLogger(MysqlUtil.class);

    private String driver;
    private String url;
    private String user;
    private String password;
    private Connection conn;

    public MysqlUtil(String dbType) {
        String propertyPrefix = "";
        if (dbType == "HIVE") {
            propertyPrefix = "hive.metastore";
        } else if (dbType == "WAREHOUSE") {
            propertyPrefix = "data.warehouse";
        }
        this.driver = PropertyFileUtil.getProperty(propertyPrefix + ".mysql.driver");
        this.url = PropertyFileUtil.getProperty(propertyPrefix + ".mysql.url");
        this.user = PropertyFileUtil.getProperty(propertyPrefix + ".mysql.user");
        this.password = PropertyFileUtil.getProperty(propertyPrefix + ".mysql.password");
    }

    private void setConn() {
        try {
            Class.forName(driver);
            this.conn = DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException classnotfoundexception) {
            classnotfoundexception.printStackTrace();
            logger.error("db: " + classnotfoundexception.getMessage());
        } catch (SQLException sqlexception) {
            logger.error("db.getconn(): " + sqlexception.getMessage());
        }
    }

    public int doInsert(String sql) throws SQLException {
        return doUpdate(sql);
    }

    public int doDelete(String sql) throws SQLException {
        return doUpdate(sql);
    }

    public int doUpdate(String sql) throws SQLException {
        Statement stmt = null;
        try {
            setConn();
            stmt = conn.createStatement();
            return stmt.executeUpdate(sql);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            close(null, stmt);
        }
    }

    public List<Map<String, Object>> doSelect(String sql) throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            setConn();
            stmt = conn.createStatement(
                    java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE,
                    java.sql.ResultSet.CONCUR_READ_ONLY);
            rs = stmt.executeQuery(sql);
            List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
            while (rs.next()) {
                Map<String, Object> map = rowToMap(rs, rs.getRow());
                list.add(map);
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            close(rs, stmt);
        }
    }


    private Map<String, Object> rowToMap(ResultSet resultset, int rowNum) throws SQLException {
        Map<String, Object> map = new HashMap<String, Object>();
        ResultSetMetaData rsmd = resultset.getMetaData();
        int columnNum = rsmd.getColumnCount();
        for (int i = 1; i <= columnNum; i++) {
            String columnName = rsmd.getColumnLabel(i);
            map.put(columnName, resultset.getObject(columnName));
        }
        return map;
    }

    public void close(ResultSet rs, Statement stmt) throws SQLException {
        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    public static void main(String[] args) {
        try {
            MysqlUtil db = new MysqlUtil("HIVE");
            List<Map<String, Object>> rs = db.doSelect("select * from t_user limit 5");
            for (Map<String, Object> map : rs) {
                for (Entry<String, Object> entry : map.entrySet()) {
                    System.out.print(entry.getKey() + ":" + entry.getValue() + ",");
                }
                System.out.println("");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
