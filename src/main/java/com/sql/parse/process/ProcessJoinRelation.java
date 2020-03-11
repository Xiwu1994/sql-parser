package com.sql.parse.process;

import com.sql.parse.bean.ParseJoinOnRelation;
import com.sql.parse.bean.ParseJoinOnSingleRelation;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import java.util.HashSet;
import java.util.Set;


public class ProcessJoinRelation {
    public static ParseJoinOnRelation process(ASTNode ast) {
        ParseJoinOnRelation parseJoinOnRelation = new ParseJoinOnRelation();

        if (ast.getText().equals("=")) {
            Set leftColumn = getColumnSet((ASTNode) ast.getChild(0));
            Set rightColumn = getColumnSet((ASTNode) ast.getChild(1));
            if (leftColumn.size() > 0 && rightColumn.size() > 0) {
                ParseJoinOnSingleRelation parseJoinOnSingleRelation = new ParseJoinOnSingleRelation();
                parseJoinOnSingleRelation.setLeftColumnSet(leftColumn);
                parseJoinOnSingleRelation.setRightColumnSet(rightColumn);

                parseJoinOnRelation.setOnOneResult(parseJoinOnSingleRelation);
            }
        } else {
            for (int i = 0; i < ast.getChildCount(); i++) {
                ParseJoinOnRelation parseOnResult1 = ProcessJoinRelation.process((ASTNode) ast.getChild(i));
                for (int j = 0; j < parseOnResult1.getOnColumnList().size(); j++) {
                    parseJoinOnRelation.setOnOneResult(parseOnResult1.getOnColumnList().get(j));
                }
            }
        }
        return parseJoinOnRelation;
    }

    public static Set getColumnSet(ASTNode ast) {
        Set<String> columnSet = new HashSet<>();
        if (ast.getText().equals(".")) {
            columnSet.add(getColumn(ast));
        } else {
            for (int i = 0; i < ast.getChildCount(); i++) {
                if (ast.getChild(i).getText().equals(".")) {
                    columnSet.add(getColumn((ASTNode) ast.getChild(i)));
                } else {
                    columnSet.addAll(getColumnSet((ASTNode) ast.getChild(i)));
                }
            }
        }
        return columnSet;
    }

    public static String getColumn(ASTNode ast) {
        String aliasName = null;
        if (ast.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
            aliasName = ast.getChild(0).getChild(0).getText();
        }
        String columnName = ast.getChild(1).getText();
        return aliasName + "." + columnName;
    }
}
