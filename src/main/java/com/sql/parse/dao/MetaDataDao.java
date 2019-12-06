package com.sql.parse.dao;

import com.sql.parse.bean.ColumnNode;
import com.sql.parse.bean.TableNode;
import com.sql.parse.exception.DBException;
import com.sql.parse.util.CheckUtil;
import com.sql.parse.util.MysqlUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class MetaDataDao {
    MysqlUtil dbUtil = new MysqlUtil();


    public List<ColumnNode> getColumn(String db, String table) {

        String sqlWhere = "TBL_NAME='" + table + "'" + (CheckUtil.isEmpty(db) ? " " : (" and NAME='" + db + "'"));
        List<ColumnNode> colList = new ArrayList<ColumnNode>();
        String sql = "SELECT RC.COLUMN_NAME,TD.TBL_NAME,TD.NAME FROM COLUMNS_V2 RC JOIN " +
                "(SELECT CD_ID,SD_ID FROM SDS) SD ON RC.CD_ID=SD.CD_ID JOIN " +
                "(SELECT RD.TBL_NAME,RD.SD_ID,DB.NAME FROM TBLS RD JOIN " +
                "DBS DB ON RD.DB_ID=DB.DB_ID WHERE " + sqlWhere + ")TD ON SD.SD_ID=TD.SD_ID " +
                "ORDER BY RC.INTEGER_IDX";

        try {
            List<Map<String, Object>> rs = dbUtil.doSelect(sql);
            for (Map<String, Object> map : rs) {
                ColumnNode column = new ColumnNode();
                column.setColumn((String) map.get("COLUMN_NAME"));
                column.setTable((String) map.get("TBL_NAME"));
                column.setDb((String) map.get("NAME"));
                colList.add(column);
            }
            return colList;
        } catch (Exception e) {
            e.printStackTrace();
            throw new DBException(sqlWhere, e);
        }
    }

    public List<TableNode> getTable(String db, String table) {

        String sqlWhere = "TBL_NAME='" + table + "'" + (CheckUtil.isEmpty(db) ? " " : (" and NAME='" + db + "'"));
        List<TableNode> list = new ArrayList<TableNode>();
        String sql = "SELECT RD.TBL_NAME,RD.SD_ID,DB.NAME FROM TBLS RD JOIN" +
                "DBS DB ON RD.DB_ID=DB.DB_ID WHERE" + sqlWhere + "";

        try {
            List<Map<String, Object>> rs = dbUtil.doSelect(sql);
            for (Map<String, Object> map : rs) {
                TableNode tableNode = new TableNode();
                tableNode.setTable((String) map.get("TBL_NAME"));
                tableNode.setDb((String) map.get("NAME"));
                list.add(tableNode);
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            throw new DBException(sqlWhere, e);
        }
    }

}
