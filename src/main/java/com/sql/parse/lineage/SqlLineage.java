package com.sql.parse.lineage;

import com.sql.parse.bean.*;
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
            ParseColumnResult parseColumnResult = getLikeByMap(parseFromResult, "."+column);
            dependencyColumns.addAll(parseColumnResult.getFromTableColumnSet());
        } else if (ast.getType() == HiveParser.Number  || ast.getType() == HiveParser.StringLiteral
                || ast.getType() == HiveParser.Identifier) {
            // 这是啥情况会进入这里
            System.out.println("wtf " + ast.getText());
        }
        return dependencyColumns;
    }

    public Set<String> processChilds(ASTNode ast, int startIndex) {
        // 依赖的字段列表
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
//            e.printStackTrace();
            System.out.print("column wtf: " + e + " ast: " + ast.getText() + "\n");
        } finally {
            return fromColumns;
        }
    }

    public Map<String, ParseColumnResult> processJoinResults(List<ParseJoinResult> joinResults) {
        Map<String, ParseColumnResult> parseFromResult = new HashMap<>();
        for (int i = 0; i < joinResults.size(); i++) {
            ParseJoinResult parseJoinResult = joinResults.get(i);
            // 处理 FROM
            if (parseJoinResult.getParseTableResults() != null) {
                List<ParseTableResult> parseTableResults = parseJoinResult.getParseTableResults();
                parseFromResult.putAll(processFromResults(parseTableResults));
            }
            // 递归处理JOIN
            if (parseJoinResult.getParseJoinResults() != null) {
                parseFromResult.putAll(processJoinResults(parseJoinResult.getParseJoinResults()));
            }
            // 处理 SUBQUERY
            if (parseJoinResult.getParseSubQueryResults() != null) {
                List<ParseSubQueryResult> parseSubQueryResults = parseJoinResult.getParseSubQueryResults();
                parseFromResult.putAll(processSubQueryResults(parseSubQueryResults));
            }
        }
        return parseFromResult;
    }

    public Map<String, ParseColumnResult> processSubQueryResults(List<ParseSubQueryResult> subQueryResults) {
        Map<String, ParseColumnResult> parseFromResult = new HashMap<>();
        for (int i = 0; i < subQueryResults.size(); i++) {
            ParseSubQueryResult parseSubQueryResult = subQueryResults.get(i);
            String subQueryAliasName = parseSubQueryResult.getAliasName();
            Map<String, ParseColumnResult> parseColumnResultMap = parseSubQueryResult.getParseSubQueryResults();

            for(Map.Entry<String, ParseColumnResult> entry : parseColumnResultMap.entrySet()){
                String columnAliasName = entry.getKey();
                ParseColumnResult parseColumnResult = entry.getValue();
                parseFromResult.put(subQueryAliasName + "." + columnAliasName, parseColumnResult);
            }
        }
        return parseFromResult;
    }

    public Map<String, ParseColumnResult> processFromResults(List<ParseTableResult> fromResults) {
        /*
        public class ParseTableResult {
            private String aliasName;
            private String tableName;
            private String dbName;
            private String tableFullName;
            private List<String> columnNameList;
        }

        转换成 parseFromResult

        Map<tableAliasName + "." + columnName, ParseColumnResult>;

        public class ParseColumnResult {
            private String aliasName;
            private Set<String> fromTableColumnSet;
        }
        * */
        Map<String, ParseColumnResult> parseFromResult = new HashMap<>();
        for (int i = 0; i < fromResults.size(); i++) {
            ParseTableResult parseTableResult = fromResults.get(i);
            String tableAliasName = parseTableResult.getAliasName();
            String tableFullName = parseTableResult.getTableFullName();
            List<String> columnNameList = parseTableResult.getColumnNameList();
            for (int j = 0; j < columnNameList.size(); j++) {
                String columnName = columnNameList.get(j);
                ParseColumnResult parseColumnResult = new ParseColumnResult();
                parseColumnResult.setAliasName(columnName);
                Set<String> fromTableColumnSet = new TreeSet<>();
                fromTableColumnSet.add(tableFullName + "." + columnName);
                parseColumnResult.setFromTableColumnSet(fromTableColumnSet);
                parseFromResult.put(tableAliasName + "." + columnName, parseColumnResult);
            }
        }
        return parseFromResult;
    }

    public void parseCurrentASTNode(ASTNode ast) {
        if(ast.getToken() == null) {
            return;
        }
        switch (ast.getToken().getType()) {
            // TOK_SUBQUERY
            case HiveParser.TOK_SUBQUERY:
                ParseSubQueryResult parseSubQueryResult = new ParseSubQueryResult();
                String subQueryAliasName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
                parseSubQueryResult.setAliasName(subQueryAliasName);

                Map<String, ParseColumnResult> selectResults = new HashMap<>();
                selectResults.putAll(parseSelectResults);
                parseSelectResults.clear();
                parseSubQueryResult.setParseSubQueryResults(selectResults);

                System.out.println("TOK_SUBQUERY: " + parseSubQueryResult);
                // 给之后的 TOK_FROM 或者 TOK_JOIN使用
                parseSubQueryResults.add(parseSubQueryResult);

                break;

            // TOK_INSERT
            case HiveParser.TOK_INSERT:
                // 整理 parseColumnResults 数据结构
                Map<String, ParseColumnResult> selectResultsTmp = new HashMap();
                for (int i = 0; i < parseColumnResults.size(); i++) {
                    selectResultsTmp.put(parseColumnResults.get(i).getAliasName(), parseColumnResults.get(i));
                }
                parseColumnResults.clear();
                // parseSelectResults 给SUBQUERY使用
                parseSelectResults.putAll(selectResultsTmp);
                System.out.println("TOK_INSERT: " + parseSelectResults);
                break;

            // TOK_FROM
            case HiveParser.TOK_FROM:
                // 整理 parseJoinResults 和 parseTableResults 数据结构， 输出让 SELECT 易用的数据结构，方便找到字段来源
                switch (((ASTNode) ast.getChild(0)).getToken().getType()) {

                    case HiveParser.TOK_SUBQUERY:
                        parseFromResult = processSubQueryResults(parseSubQueryResults);
                        parseSubQueryResults.clear();
                        break;

                    case HiveParser.TOK_TABREF:
                        parseFromResult = processFromResults(parseTableResults);
                        parseTableResults.clear();
                        break;

                    case HiveParser.TOK_RIGHTOUTERJOIN:
                    case HiveParser.TOK_LEFTOUTERJOIN:
                    case HiveParser.TOK_JOIN:
                    case HiveParser.TOK_LEFTSEMIJOIN:
                    case HiveParser.TOK_MAPJOIN:
                    case HiveParser.TOK_FULLOUTERJOIN:
                    case HiveParser.TOK_UNIQUEJOIN:
                        parseFromResult = processJoinResults(parseJoinResults);
                        parseJoinResults.clear();
                        break;
                    default:
                        break;
                }

                System.out.println("TOK_FROM: " + parseFromResult);
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

                System.out.println("TOK_JOIN: " + parseJoinResult);

                // 为了给之后的 JOIN 或者 SUB_QUERY 或者 FROM
                parseJoinResults.add(parseJoinResult);

                break;

            // TOK_TABREF
            case HiveParser.TOK_TABREF:
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
                System.out.println("TOK_TABREF: " + parseTableResult);

                // 为了给之后的 JOIN 使用
                parseTableResults.add(parseTableResult);
                break;

            // SELECT
            case HiveParser.TOK_SELEXPR:
                Set<String> fromColumnSet = parseSelect((ASTNode) ast.getChild(0));
                ASTNode childAst = (ASTNode) ast.getChild(0);
                String columnAliasName = null;
                if (ast.getChild(1) != null) {
                    // 有字段别名
                    columnAliasName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
                } else if (childAst.getType() == HiveParser.DOT && childAst.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
                        && childAst.getChild(0).getChildCount() == 1 && childAst.getChild(1).getType() == HiveParser.Identifier) {
                    // 没有别名，但是使用的 t1.xx  格式
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
                System.out.println("TOK_SELEXPR: " + parseColumnResult);

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

    public void endParse() {
        String table = "secoo_dim.dim_product_category";
        MetaCacheUtil.getInstance().init(table);
        List<String> list = MetaCacheUtil.getInstance().getColumnByDBAndTable(table);
        System.out.println(list);
    }

    public void parse(String sql) throws Exception {
        ASTNode ast = getASTNode(sql);
        parseASTNode(ast);
//        endParse();
    }
}
