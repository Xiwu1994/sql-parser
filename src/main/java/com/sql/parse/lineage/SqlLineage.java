package com.sql.parse.lineage;

import com.sql.parse.util.MetaCacheUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import java.util.List;


public class SqlLineage {

    public static ASTNode getASTNode(String sql) throws Exception {
        HiveConf hiveConf = new HiveConf();
        Configuration conf = new Configuration(hiveConf);
        conf.set("_hive.hdfs.session.path","/tmp");
        conf.set("_hive.local.session.path","/tmp");
        Context context = new Context(conf);
        ParseDriver pd = new ParseDriver();
        ASTNode ast = pd.parse(sql, context);
        System.out.println(ast.dump());
        return ast;
    }


    public static void parseChildASTNode(ASTNode ast) {
        int childCount = ast.getChildCount();
        for (int i = 0; i < childCount; i++) {
            parseASTNode((ASTNode) ast.getChild(i));
        }
    }

    public static void parseSelectColumn(ASTNode ast) {
        if (ast.getType() == HiveParser.DOT && ast.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
                && ast.getChild(0).getChildCount() == 1 && ast.getChild(1).getType() == HiveParser.Identifier) {
            // 字段 有别名
            String column = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
            String alia = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0).getChild(0));
            System.out.println(alia + "." + column);
        } else if (ast.getType() == HiveParser.TOK_TABLE_OR_COL  && ast.getChildCount() == 1
                && ast.getChild(0).getType() == HiveParser.Identifier) {
            // 字段 无别名
            String column = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
            System.out.println(column);
        } else if (ast.getType() == HiveParser.Number  || ast.getType() == HiveParser.StringLiteral
                || ast.getType() == HiveParser.Identifier) {
            // 这是啥情况会进入这里
            System.out.println("wtf " + ast.getText());
        }
    }

    public static void processChilds(ASTNode ast, int startIndex) {
        int cnt = ast.getChildCount();
        for (int i = startIndex; i < cnt; i++) {
            parseSelect((ASTNode) ast.getChild(i));
        }
    }

    public static void parseSelect(ASTNode ast) {
        try {
            if (ast.getType() == HiveParser.KW_OR
                    || ast.getType() == HiveParser.KW_AND) {
                parseSelect((ASTNode) ast.getChild(0));
                parseSelect((ASTNode) ast.getChild(1));
            } else if (ast.getType() == HiveParser.NOTEQUAL || ast.getType() == HiveParser.EQUAL
                    || ast.getType() == HiveParser.LESSTHAN || ast.getType() == HiveParser.LESSTHANOREQUALTO
                    || ast.getType() == HiveParser.GREATERTHAN || ast.getType() == HiveParser.GREATERTHANOREQUALTO
                    || ast.getType() == HiveParser.KW_LIKE || ast.getType() == HiveParser.DIVIDE
                    || ast.getType() == HiveParser.PLUS || ast.getType() == HiveParser.MINUS
                    || ast.getType() == HiveParser.STAR || ast.getType() == HiveParser.MOD
                    || ast.getType() == HiveParser.AMPERSAND || ast.getType() == HiveParser.TILDE
                    || ast.getType() == HiveParser.BITWISEOR || ast.getType() == HiveParser.BITWISEXOR) {
                parseSelect((ASTNode) ast.getChild(0));
                if (ast.getChild(1) == null) { // -1

                } else {
                    parseSelect((ASTNode) ast.getChild(1));
                }
            } else if (ast.getType() == HiveParser.TOK_FUNCTIONDI) {
                parseSelect((ASTNode) ast.getChild(1));
            } else if (ast.getType() == HiveParser.TOK_FUNCTION) {
                String fun = ast.getChild(0).getText();
                if (ast.getChild(1) != null) {
                    parseSelect((ASTNode) ast.getChild(1));
                }
                if ("when".equalsIgnoreCase(fun)) {
                    processChilds(ast, 1);
                } else if ("IN".equalsIgnoreCase(fun)) {
                    processChilds(ast, 2);
                } else if ("TOK_ISNOTNULL".equalsIgnoreCase(fun)
                        || "TOK_ISNULL".equalsIgnoreCase(fun)) {

                } else if ("BETWEEN".equalsIgnoreCase(fun)) {
                    parseSelect((ASTNode) ast.getChild(2));
                    parseSelect((ASTNode) ast.getChild(3));
                    parseSelect((ASTNode) ast.getChild(4));
                }
                processChilds(ast, 1);
            } else if (ast.getType() == HiveParser.LSQUARE) {
                parseSelect((ASTNode) ast.getChild(0));
                parseSelect((ASTNode) ast.getChild(1));
            } else {
                parseSelectColumn(ast);
            }
        } catch (Exception e) {
            System.out.print("wtf: " + e + " ast: " + ast.getText());
        }
    }

    public static void parseCurrentASTNode(ASTNode ast) {
        if(ast.getToken() == null) {
            return;
        }
        switch (ast.getToken().getType()) {
            // FROM
            case HiveParser.TOK_TABREF:
                String dbName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0).getChild(0));
                String tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0).getChild(1));
                System.out.println(dbName + "." + tableName);
                break;
            // SELECT
            case HiveParser.TOK_SELEXPR:
                parseSelect((ASTNode) ast.getChild(0));
            default:
                break;
        }
    }

    public static void parseASTNode(ASTNode ast) {
        // 递归处理，优先处理子节点
        parseChildASTNode(ast);
        // 子节点都处理完后，处理当前节点
        parseCurrentASTNode(ast);
    }

    public static void endParse() {
        String table = "secoo_dim.dim_product_category";
        MetaCacheUtil.getInstance().init(table);
        List<String> list = MetaCacheUtil.getInstance().getColumnByDBAndTable(table);
        System.out.println(list);
    }


    public static void parse(String sql) throws Exception {
        ASTNode ast = getASTNode(sql);
        parseASTNode(ast);
//        endParse();
    }
}
