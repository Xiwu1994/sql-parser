package com.sql.parse.lineage;

import com.sql.parse.bean.*;
import com.sql.parse.process.*;
import com.sql.parse.util.MetaCacheUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import java.util.*;

public class SqlLineage {
    private List<ParseColumnResult> parseColumnResults = new ArrayList();
    private List<ParseTableResult> parseTableResults = new ArrayList();
    private List<ParseJoinResult> parseJoinResults = new ArrayList();
    private List<ParseSubQueryResult> parseSubQueryResults = new ArrayList();

    // insert into table's column list
    private List<String> insertTableColumns = new ArrayList<>();

    private Map<String, ParseColumnResult> parseSelectResults = new HashMap<>();
    private Map<String, ParseColumnResult> parseFromResult = new HashMap<>();

    public ASTNode getASTNode(String sql) throws Exception {
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

    public void parseChildASTNode(ASTNode ast) {
        int childCount = ast.getChildCount();
        for (int i = 0; i < childCount; i++) {
            parseASTNode((ASTNode) ast.getChild(i));
        }
    }

    public void parseCurrentASTNode(ASTNode ast) {
        if(ast.getToken() == null) {
            return;
        }
        switch (ast.getToken().getType()) {
            // CREATE TABLE AS 入库表名
            case HiveParser.TOK_CREATETABLE:
                String createTableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                System.out.println(createTableName);
                MetaCacheUtil.getInstance().init(createTableName);
                insertTableColumns = MetaCacheUtil.getInstance().getColumnByDBAndTable(createTableName);
                // 终点 create table as 步骤
                for (int i=0; i<insertTableColumns.size(); i++) {
                    String createTableColumnName = insertTableColumns.get(i);
                    Set createFromTableColumnSet = parseSelectResults.get(createTableColumnName).getFromTableColumnSet();
                    System.out.println("字段：" + createTableColumnName + " 依赖字段: " + createFromTableColumnSet);
                }
                break;
            // INSERT 入库表名
            case HiveParser.TOK_TAB:
                String insertTableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                MetaCacheUtil.getInstance().init(insertTableName);
                insertTableColumns = MetaCacheUtil.getInstance().getColumnByDBAndTable(insertTableName);
                break;
            // TOK_SUBQUERY 子查询
            case HiveParser.TOK_SUBQUERY:
                ParseSubQueryResult parseSubQueryResult = new ParseSubQueryResult();
                String subQueryAliasName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
                parseSubQueryResult.setAliasName(subQueryAliasName);

                Map<String, ParseColumnResult> selectResults = new HashMap<>();
                selectResults.putAll(parseSelectResults);
                parseSelectResults.clear();
                parseSubQueryResult.setParseSubQueryResults(selectResults);

                // System.out.println("TOK_SUBQUERY: " + parseSubQueryResult);
                // 给之后的 TOK_FROM 或者 TOK_JOIN使用
                parseSubQueryResults.add(parseSubQueryResult);

                break;

            // TOK_INSERT
            case HiveParser.TOK_INSERT:
                // 整理 parseColumnResults 数据结构
                Map<String, ParseColumnResult> selectResultsTmp = new HashMap();

                if (insertTableColumns.size() > 0) {
                    // 终点： insert into table 步骤
                    for (int i = 0; i < parseColumnResults.size(); i++) {
                        String insertTableColumnName = insertTableColumns.get(i);
                        Set<String> insertFromTableColumnSet = parseColumnResults.get(i).getFromTableColumnSet();
                        System.out.println("字段：" + insertTableColumnName + " 依赖字段: " + insertFromTableColumnSet);
                    }
                    parseColumnResults.clear();
                    insertTableColumns.clear();
                } else {
                    for (int i = 0; i < parseColumnResults.size(); i++) {
                        selectResultsTmp.put(parseColumnResults.get(i).getAliasName(), parseColumnResults.get(i));
                    }
                    parseColumnResults.clear();
                    // parseSelectResults 给SUBQUERY使用
                    parseSelectResults.putAll(selectResultsTmp);
                    // System.out.println("TOK_INSERT: " + parseSelectResults);
                }
                break;

            // TOK_FROM
            case HiveParser.TOK_FROM:
                // 整理 parseJoinResults 和 parseTableResults 数据结构， 输出让 SELECT 易用的数据结构，方便找到字段来源
                switch (((ASTNode) ast.getChild(0)).getToken().getType()) {

                    case HiveParser.TOK_SUBQUERY:
                        parseFromResult = ProcessSubQueryData.process(parseSubQueryResults);
                        parseSubQueryResults.clear();
                        break;

                    case HiveParser.TOK_TABREF:
                        parseFromResult = ProcessFromData.process(parseTableResults);
                        parseTableResults.clear();
                        break;

                    case HiveParser.TOK_RIGHTOUTERJOIN:
                    case HiveParser.TOK_LEFTOUTERJOIN:
                    case HiveParser.TOK_JOIN:
                    case HiveParser.TOK_LEFTSEMIJOIN:
                    case HiveParser.TOK_MAPJOIN:
                    case HiveParser.TOK_FULLOUTERJOIN:
                    case HiveParser.TOK_UNIQUEJOIN:
                        parseFromResult = ProcessJoinData.process(parseJoinResults);
                        parseJoinResults.clear();
                        break;
                    default:
                        break;
                }

                // System.out.println("TOK_FROM: " + parseFromResult);
                break;

            // TOK_JOIN
            case HiveParser.TOK_RIGHTOUTERJOIN:
            case HiveParser.TOK_LEFTOUTERJOIN:
            case HiveParser.TOK_JOIN:
            case HiveParser.TOK_LEFTSEMIJOIN:
            case HiveParser.TOK_MAPJOIN:
            case HiveParser.TOK_FULLOUTERJOIN:
            case HiveParser.TOK_UNIQUEJOIN:
                // 生成 ParseJoinResult
                ParseJoinResult parseJoinResult = new ParseJoinResult();

                // 处理 from table
                List<ParseTableResult> tableResults = new ArrayList<>();
                tableResults.addAll(parseTableResults);
                parseTableResults.clear();
                parseJoinResult.setParseTableResults(tableResults);

                // 处理 join (table or subquery)
                List<ParseJoinResult> joinResults = new ArrayList<>();
                joinResults.addAll(parseJoinResults);
                parseJoinResults.clear();
                parseJoinResult.setParseJoinResults(joinResults);

                // 处理 subquery
                List<ParseSubQueryResult> subQueryResults = new ArrayList<>();
                subQueryResults.addAll(parseSubQueryResults);
                parseSubQueryResults.clear();
                parseJoinResult.setParseSubQueryResults(subQueryResults);

                // System.out.println("TOK_JOIN: " + parseJoinResult);

                // 为了给之后的 JOIN 或者 SUB_QUERY 或者 FROM
                parseJoinResults.add(parseJoinResult);

                break;

            // TOK_TABREF
            case HiveParser.TOK_TABREF:
                ParseTableResult parseTableResult = ProcessTokTabref.process(ast);

                // 为了给之后的 JOIN 使用
                parseTableResults.add(parseTableResult);
                break;

            // SELECT
            case HiveParser.TOK_SELEXPR:
                ProcessTokSelexpr processTokSelexpr = new ProcessTokSelexpr();
                processTokSelexpr.setParseFromResult(parseFromResult);
                ParseColumnResult parseColumnResult = processTokSelexpr.process(ast);

                // 为了给之后的 INSERT 使用
                parseColumnResults.add(parseColumnResult);
                break;
            default:
                break;
        }
    }

    public void parseASTNode(ASTNode ast) {
        // 递归处理，优先处理子节点
        parseChildASTNode(ast);
        // 子节点都处理完后，处理当前节点
        parseCurrentASTNode(ast);
    }

    public void parse(String sql) throws Exception {
        ASTNode ast = getASTNode(sql);
        parseASTNode(ast);
    }
}
