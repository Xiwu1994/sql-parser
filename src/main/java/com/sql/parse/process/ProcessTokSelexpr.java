package com.sql.parse.process;

import com.sql.parse.bean.ParseColumnResult;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.log4j.Logger;

import java.util.*;

public class ProcessTokSelexpr {
    private static Logger logger = Logger.getLogger(ProcessTokSelexpr.class);

    private Map<String, ParseColumnResult> parseFromResult;

    public void setParseFromResult(Map<String, ParseColumnResult> parseFromResult) {
        this.parseFromResult = parseFromResult;
    }

    public static ParseColumnResult getResultByColumn(Map<String, ParseColumnResult> map, String column){
        ParseColumnResult parseColumnResult = null;
        for (Map.Entry<String, ParseColumnResult> entity : map.entrySet()) {
            String columnName = entity.getKey();
            if (columnName.contains(".")) {
                columnName = columnName.split("\\.")[1];
            }
            if (columnName.equals(column)) {
                parseColumnResult = entity.getValue();
            }
        }
        return parseColumnResult;
    }

    public Set<String> parseSelect(ASTNode ast) {
        // 依赖的字段列表
        Set<String> fromColumns = new TreeSet();

        if (ast.getType() == HiveParser.DOT && ast.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
                && ast.getChild(0).getChildCount() == 1 && ast.getChild(1).getType() == HiveParser.Identifier) {
            // 字段 有别名
            String column = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
            String alias = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0).getChild(0));

            String columnFull = alias + "." + column;
            if (parseFromResult.containsKey(columnFull)) {
                fromColumns.addAll(parseFromResult.get(columnFull).getFromTableColumnSet());
            } else {
                logger.error("columnFull: " + columnFull + "has no source..");
            }
        } else if (ast.getType() == HiveParser.TOK_TABLE_OR_COL && ast.getChildCount() == 1
                && ast.getChild(0).getType() == HiveParser.Identifier) {
            // 字段 无别名
            String column = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
            ParseColumnResult parseColumnResult = getResultByColumn(parseFromResult, column);
            if (parseColumnResult != null) {
                fromColumns.addAll(parseColumnResult.getFromTableColumnSet());
            }
        } else {
            int cnt = ast.getChildCount();
            for (int i = 0; i < cnt; i++) {
                fromColumns.addAll(parseSelect((ASTNode) ast.getChild(i)));
            }
        }
        return fromColumns;
    }

    public String getColumnAliasName(ASTNode ast) {
        int childIndex = ast.getChildIndex();

        ASTNode childAst = (ASTNode) ast.getChild(0);
        String columnAliasName;
        if (ast.getChild(1) != null) {
            // 有字段别名
            columnAliasName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
        } else if (childAst.getType() == HiveParser.DOT && childAst.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
                && childAst.getChild(0).getChildCount() == 1 && childAst.getChild(1).getType() == HiveParser.Identifier) {
            // 没有别名，但是使用的 t1.xx 格式
            String columnName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) childAst.getChild(1));
            columnAliasName = columnName;
        } else if (childAst.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
            // select column
            columnAliasName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) childAst.getChild(0));
        } else {
            // 使用的元数据获取到的 insert table 的字段
            columnAliasName = "col_" + childIndex;
        }
        return columnAliasName;
    }

    private boolean hasAllcolref(ASTNode ast) {
        boolean flag = false;
        for (int i = 0; i < ast.getParent().getChildCount() - ast.getChildIndex(); i++) {
            if (ast.getParent().getChild(i).getChild(0).getType() == HiveParser.TOK_ALLCOLREF) {
                flag = true;
                break;
            }
        }
        return flag;
    }

    private int getChildIndex(ASTNode ast) {
        int childIndex;
        if (hasAllcolref(ast)) {
            childIndex = ast.getChildIndex() + parseFromResult.size() - 1;
        } else {
            childIndex = ast.getChildIndex();
        }
        return childIndex;
    }

    public ParseColumnResult process(ASTNode ast) {
        Set<String> fromColumnSet = parseSelect((ASTNode) ast.getChild(0));
        String columnAliasName = getColumnAliasName(ast);

        ParseColumnResult parseColumnResult = new ParseColumnResult();
        parseColumnResult.setIndex(getChildIndex(ast));
        parseColumnResult.setAliasName(columnAliasName);
        parseColumnResult.setFromTableColumnSet(fromColumnSet);

        return parseColumnResult;
    }
}
