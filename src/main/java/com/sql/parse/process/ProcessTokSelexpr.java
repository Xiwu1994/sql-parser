package com.sql.parse.process;

import com.sql.parse.bean.ParseColumnResult;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import java.util.*;

public class ProcessTokSelexpr {
    private Map<String, ParseColumnResult> parseFromResult;

    public void setParseFromResult(Map<String, ParseColumnResult> parseFromResult) {
        this.parseFromResult = parseFromResult;
    }

    public static ParseColumnResult getLikeByMap(Map<String, ParseColumnResult> map, String keyLike){
        ParseColumnResult parseColumnResult = null;
        for (Map.Entry<String, ParseColumnResult> entity : map.entrySet()) {
            if(entity.getKey().indexOf(keyLike) > -1){
                parseColumnResult = entity.getValue();
            }
        }
        return parseColumnResult;
    }

    public Set<String> parseSelectColumn(ASTNode ast) {
        Set<String> dependencyColumns = new TreeSet<>();
        if (ast.getType() == HiveParser.DOT && ast.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
                && ast.getChild(0).getChildCount() == 1 && ast.getChild(1).getType() == HiveParser.Identifier) {
            // 字段 有别名
            String column = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
            String alias = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0).getChild(0));

            // 字段的依赖情况
            dependencyColumns.addAll(parseFromResult.get(alias + "." + column).getFromTableColumnSet());
        } else if (ast.getType() == HiveParser.TOK_TABLE_OR_COL  && ast.getChildCount() == 1
                && ast.getChild(0).getType() == HiveParser.Identifier) {
            // 字段 无别名
            // 假设只会From一个表，如果出现了多个表，则报警。 之后可以改成从元数据里读取对应字段 判断来自拿个表
            String column = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
            ParseColumnResult parseColumnResult = getLikeByMap(parseFromResult, "." + column);
            dependencyColumns.addAll(parseColumnResult.getFromTableColumnSet());
        } else if (ast.getType() == HiveParser.Number  || ast.getType() == HiveParser.StringLiteral
                || ast.getType() == HiveParser.Identifier) {
            // 这是啥情况会进入这里
            System.out.println("WARN.. COLUMN: " + ast.getText());
        }
        return dependencyColumns;
    }

    public Set<String> processChilds(ASTNode ast, int startIndex) {
        // 依赖的字段列表processChilds
        Set<String> fromColumns = new TreeSet();
        int cnt = ast.getChildCount();
        for (int i = startIndex; i < cnt; i++) {
            fromColumns.addAll(parseSelect((ASTNode) ast.getChild(i)));
        }
        return fromColumns;
    }

    public Set<String> parseSelect(ASTNode ast) {
        // 依赖的字段列表
        Set<String> fromColumns = new TreeSet();
        try {
            if (ast.getType() == HiveParser.KW_OR
                    || ast.getType() == HiveParser.KW_AND) {
                fromColumns.addAll(parseSelect((ASTNode) ast.getChild(0)));
                fromColumns.addAll(parseSelect((ASTNode) ast.getChild(1)));
            } else if (ast.getType() == HiveParser.NOTEQUAL || ast.getType() == HiveParser.EQUAL
                    || ast.getType() == HiveParser.LESSTHAN || ast.getType() == HiveParser.LESSTHANOREQUALTO
                    || ast.getType() == HiveParser.GREATERTHAN || ast.getType() == HiveParser.GREATERTHANOREQUALTO
                    || ast.getType() == HiveParser.KW_LIKE || ast.getType() == HiveParser.DIVIDE
                    || ast.getType() == HiveParser.PLUS || ast.getType() == HiveParser.MINUS
                    || ast.getType() == HiveParser.STAR || ast.getType() == HiveParser.MOD
                    || ast.getType() == HiveParser.AMPERSAND || ast.getType() == HiveParser.TILDE
                    || ast.getType() == HiveParser.BITWISEOR || ast.getType() == HiveParser.BITWISEXOR) {
                fromColumns.addAll(parseSelect((ASTNode) ast.getChild(0)));
                if (ast.getChild(1) == null) { // -1

                } else {
                    fromColumns.addAll(parseSelect((ASTNode) ast.getChild(1)));
                }
            } else if (ast.getType() == HiveParser.TOK_FUNCTIONDI) {
                fromColumns.addAll(parseSelect((ASTNode) ast.getChild(1)));
            } else if (ast.getType() == HiveParser.TOK_FUNCTION) {
                String fun = ast.getChild(0).getText();
                if (ast.getChild(1) != null) {
                    fromColumns.addAll(parseSelect((ASTNode) ast.getChild(1)));
                }
                if ("when".equalsIgnoreCase(fun)) {
                    fromColumns.addAll(processChilds(ast, 1));
                } else if ("IN".equalsIgnoreCase(fun)) {
                    fromColumns.addAll(processChilds(ast, 2));
                } else if ("TOK_ISNOTNULL".equalsIgnoreCase(fun)
                        || "TOK_ISNULL".equalsIgnoreCase(fun)) {

                } else if ("BETWEEN".equalsIgnoreCase(fun)) {
                    fromColumns.addAll(parseSelect((ASTNode) ast.getChild(2)));
                    fromColumns.addAll(parseSelect((ASTNode) ast.getChild(3)));
                    fromColumns.addAll(parseSelect((ASTNode) ast.getChild(4)));
                }
                fromColumns.addAll(processChilds(ast, 1));
            } else if (ast.getType() == HiveParser.LSQUARE) {
                fromColumns.addAll(parseSelect((ASTNode) ast.getChild(0)));
                fromColumns.addAll(parseSelect((ASTNode) ast.getChild(1)));
            } else {
                fromColumns.addAll(parseSelectColumn(ast));
            }
        } catch (Exception e) {
            System.out.print("column wtf: " + e + " ast: " + ast.getText() + "\n");
        } finally {
            return fromColumns;
        }
    }

    public ParseColumnResult process(ASTNode ast) {
        Set<String> fromColumnSet = parseSelect((ASTNode) ast.getChild(0));
        ASTNode childAst = (ASTNode) ast.getChild(0);
        String columnAliasName = null;
        if (ast.getChild(1) != null) {
            // 有字段别名
            columnAliasName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
        } else if (childAst.getType() == HiveParser.DOT && childAst.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
                && childAst.getChild(0).getChildCount() == 1 && childAst.getChild(1).getType() == HiveParser.Identifier) {
            // 没有别名，但是使用的 t1.xx 格式
            String columnName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) childAst.getChild(1));
            columnAliasName = columnName;
        } else {
            Iterator<String> it = fromColumnSet.iterator();
            while (it.hasNext()) {
                String fromTableColumnFullName = it.next();
                columnAliasName = fromTableColumnFullName.split("\\.")[fromTableColumnFullName.split("\\.").length - 1];
                break;
            }
        }
        ParseColumnResult parseColumnResult = new ParseColumnResult();
        parseColumnResult.setAliasName(columnAliasName);
        parseColumnResult.setFromTableColumnSet(fromColumnSet);
        return parseColumnResult;
    }
}
