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

    private List<Map<String, ParseColumnResult>> parseQueryResults = new ArrayList<>();
    private Map<String, ParseColumnResult> parseSelectResults = new HashMap<>();
    private Map<String, ParseColumnResult> parseUnionColumnResults = new HashMap<>();
    private Map<String, ParseColumnResult> parseFromResult = new HashMap<>();
    private Map<String, ParseColumnResult> parseLateralViewResult = new HashMap<>();


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
        if (ast.getToken() != null && (ast.getToken().getType() == HiveParser.TOK_LATERAL_VIEW ||
                ast.getToken().getType() == HiveParser.TOK_LATERAL_VIEW_OUTER)) {
            // later view 比较特殊，需要先处理 第二个节点，再返回处理 第一个节点
            parseASTNode((ASTNode) ast.getChild(1));
            parseASTNode((ASTNode) ast.getChild(0));
        } else {
            int childCount = ast.getChildCount();
            for (int i = 0; i < childCount; i++) {
                parseASTNode((ASTNode) ast.getChild(i));
            }
        }
    }

    public Map<String, ParseColumnResult> genFromColumnData(ASTNode ast) {
        Map<String, ParseColumnResult> fromColumnDataMap = null;
        switch (ast.getToken().getType()) {
            case HiveParser.TOK_SUBQUERY:
                fromColumnDataMap = ProcessSubQueryData.process(parseSubQueryResults);
                parseSubQueryResults.clear();
                break;

            case HiveParser.TOK_TABREF:
                fromColumnDataMap = ProcessTabrefData.process(parseTableResults);
                parseTableResults.clear();
                break;

            case HiveParser.TOK_RIGHTOUTERJOIN:
            case HiveParser.TOK_LEFTOUTERJOIN:
            case HiveParser.TOK_JOIN:
            case HiveParser.TOK_LEFTSEMIJOIN:
            case HiveParser.TOK_MAPJOIN:
            case HiveParser.TOK_FULLOUTERJOIN:
            case HiveParser.TOK_UNIQUEJOIN:
                fromColumnDataMap = ProcessJoinData.process(parseJoinResults);
                parseJoinResults.clear();
                break;

            case HiveParser.TOK_LATERAL_VIEW:
            case HiveParser.TOK_LATERAL_VIEW_OUTER:
                Map<String, ParseColumnResult> lateralViewResultTmp = new HashMap<>();
                lateralViewResultTmp.putAll(parseLateralViewResult);
                parseLateralViewResult.clear();
                fromColumnDataMap = lateralViewResultTmp;
                break;

            default:
                break;
        }
        return fromColumnDataMap;
    }

    public void parseCurrentASTNode(ASTNode ast) {
        if(ast.getToken() == null) {
            return;
        }
        switch (ast.getToken().getType()) {
            // CREATE TABLE AS 入库表名
            case HiveParser.TOK_CREATETABLE:
                String createTableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
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
                // from TOK_UNIONALL or TOK_QUERY
                // to TOK_FROM or TOK_JOIN
                Map<String, ParseColumnResult> subQueryColumnMap = null;
                if (parseUnionColumnResults.size() > 0) {
                    // 有UNION ALL 操作
                    subQueryColumnMap = parseUnionColumnResults;
                } else {
                    // parseQueryResults parseQueryResults
                    subQueryColumnMap = parseQueryResults.get(0);
                }
                ParseSubQueryResult parseSubQueryResult = new ParseSubQueryResult();
                String subQueryAliasName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
                parseSubQueryResult.setAliasName(subQueryAliasName);

                Map<String, ParseColumnResult> selectResults = new HashMap<>();
                selectResults.putAll(subQueryColumnMap);
                parseSubQueryResult.setParseSubQueryResults(selectResults);

                parseUnionColumnResults.clear();
                parseQueryResults.clear();
//                System.out.println("TOK_SUBQUERY: " + parseSubQueryResult);
                parseSubQueryResults.add(parseSubQueryResult);

                break;

            case HiveParser.TOK_UNIONALL:
                // from TOK_QUERY
                // to TOK_SUBQUERY
                Map<String, ParseColumnResult> newParseColumnResultMap = new HashMap<>();

                Map<String, ParseColumnResult> parseColumnResultMap = parseQueryResults.get(0);

                for(Map.Entry<String, ParseColumnResult> entry : parseColumnResultMap.entrySet()){
                    String columnAliasName = entry.getKey();
                    ParseColumnResult parseColumnResult = entry.getValue();

                    for (int i=1; i<parseQueryResults.size(); i++) {
                        Set<String> otherUnionFromColumnSet = parseQueryResults.get(i).get(columnAliasName).getFromTableColumnSet();
                        parseColumnResult.getFromTableColumnSet().addAll(otherUnionFromColumnSet);
                        newParseColumnResultMap.put(columnAliasName, parseColumnResult);
                    }
                }
                parseUnionColumnResults.putAll(newParseColumnResultMap);
                parseQueryResults.clear();
                break;

            case HiveParser.TOK_QUERY:
                // from TOK_INSERT
                // to TOK_SUBQUERY or TOK_UNIONALL
                Map<String, ParseColumnResult> queryColumnMapTmp = new HashMap<>();
                queryColumnMapTmp.putAll(parseSelectResults);
                parseQueryResults.add(queryColumnMapTmp);
                parseSelectResults.clear();
                break;

            case HiveParser.TOK_INSERT:
                // from TOK_SELEXPR
                // to TOK_QUERY
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
                    parseSelectResults.putAll(selectResultsTmp);
//                    System.out.println("TOK_INSERT: " + selectResultsTmp);
                }
                break;

            // TOK_LATERAL_VIEW 行转列
            case HiveParser.TOK_LATERAL_VIEW:
            case HiveParser.TOK_LATERAL_VIEW_OUTER:
                // from TOK_SUBQUERY or TOK_TABREF or TOK_JOIN
                // to TOK_FROM
                parseLateralViewResult = genFromColumnData((ASTNode) ast.getChild(1));

                // 把 lateral view 生成的字段也 放进fromTableColumnMap结构里
                ProcessTokSelexpr laterViewTokSelexpr = new ProcessTokSelexpr();
                laterViewTokSelexpr.setParseFromResult(parseLateralViewResult);
                ParseColumnResult laterViewParseColumnResult = laterViewTokSelexpr.process((ASTNode) ast.getChild(0).getChild(0));
                parseLateralViewResult.put('.' + laterViewParseColumnResult.getAliasName(), laterViewParseColumnResult);

                // 清理 TOK_SELEXPR
                parseColumnResults.clear();

                break;

            // TOK_FROM
            case HiveParser.TOK_FROM:
                // from TOK_SUBQUERY or TOK_TABREF or TOK_JOIN or TOK_LATERAL_VIEW
                // to TOK_SELEXPR
                parseFromResult = genFromColumnData((ASTNode) ast.getChild(0));
//                System.out.println("TOK_FROM: " + parseFromResult);
                break;

            // TOK_JOIN
            case HiveParser.TOK_RIGHTOUTERJOIN:
            case HiveParser.TOK_LEFTOUTERJOIN:
            case HiveParser.TOK_JOIN:
            case HiveParser.TOK_LEFTSEMIJOIN:
            case HiveParser.TOK_MAPJOIN:
            case HiveParser.TOK_FULLOUTERJOIN:
            case HiveParser.TOK_UNIQUEJOIN:
                // from TOK_JOIN or TOK_FROM or TOK_SUBQUERY
                // to TOK_JOIN or TOK_FROM to TOK_SUBQUERY
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

//                System.out.println("TOK_JOIN: " + parseJoinResult);

                // 为了给之后的 JOIN 或者 SUB_QUERY 或者 FROM
                parseJoinResults.add(parseJoinResult);

                break;

            // TOK_TABREF
            case HiveParser.TOK_TABREF:
                // to TOK_FROM or TOK_JOIN
                ParseTableResult parseTableResult = ProcessTokTabref.process(ast);
//                System.out.println("TOK_TABREF: " + parseTableResult);

                parseTableResults.add(parseTableResult);
                break;

            // SELECT
            case HiveParser.TOK_SELEXPR:
                // from TOK_FROM
                // to TOK_INSERT
                ProcessTokSelexpr processTokSelexpr = new ProcessTokSelexpr();
                processTokSelexpr.setParseFromResult(parseFromResult);
                ParseColumnResult parseColumnResult = processTokSelexpr.process(ast);

//                System.out.println("TOK_SELEXPR: " + parseColumnResult);
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
