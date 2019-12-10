package com.sql.parse.process;


import com.sql.parse.bean.ParseTableResult;
import com.sql.parse.util.MetaCacheUtil;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;

public class ProcessTokTabref {
    public static ParseTableResult process(ASTNode ast) {
        String dbName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0).getChild(0));
        String tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0).getChild(1));
        String tableFullName = dbName + "." + tableName;
        // 判断是否有别名
        String tableAliasName;
        if (ast.getChild(1) != null) {
            tableAliasName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
        } else {
            // 没有别名用库+表名
            tableAliasName = dbName + "__" + tableName;
        }
        // 生成 ParseTableResult
        ParseTableResult parseTableResult = new ParseTableResult();
        parseTableResult.setAliasName(tableAliasName);
        parseTableResult.setDbName(dbName);
        parseTableResult.setTableName(tableName);
        parseTableResult.setTableFullName(tableFullName);
        MetaCacheUtil.getInstance().init(tableFullName);
        parseTableResult.setColumnNameList(MetaCacheUtil.getInstance().getColumnByDBAndTable(tableFullName));
        // System.out.println("TOK_TABREF: " + parseTableResult);
        return parseTableResult;
    }
}
