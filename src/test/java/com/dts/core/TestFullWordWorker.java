package com.dts.core;

import com.dts.core.common.meta.*;
import com.dts.core.common.sql.SqlTemplates;
import com.dts.core.common.v1.DbType;
import com.dts.core.common.v1.RunMode;
import com.dts.core.exception.DtsException;
import com.dts.core.model.ApplicationValue;
import com.dts.core.model.DataTaskResult;
import com.dts.core.util.DataSourceFactory;
import com.dts.core.util.DtsUtils;
import com.dts.core.util.DtsWorkerFactory;
import com.dts.core.util.ForkJoinUtils;
import com.dts.core.worker.AbstractRecordWorker;
import com.dts.core.worker.AbstractRecordWorker.*;
import com.google.common.collect.Lists;
import com.google.common.collect.MigrateMap;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ForkJoinPool;

public class TestFullWordWorker {
    protected Map<List<String>, TableSqlUnit> applierSqlCache;
    protected ApplicationValue applicationValue;
    protected Boolean useMerge = false;

    JdbcTemplate jdbcTemplateApp;

    @Before
    public void initSqlCache(){
        System.out.println("------------------初始化执行生成makeMap数据结构------------------");
        applierSqlCache = MigrateMap.makeMap();
        applicationValue = new ApplicationValue();
        DataSourceMeta   sourceMeta = new DataSourceMeta();
        sourceMeta.setSchemaName("test_dataload");
        sourceMeta.setDbType(DbType.ORACLE);
        DataSourceFactory dataSourceFactory = DataSourceFactory.getDataSourceFactory();
        DataSource dataSource = dataSourceFactory.createDataSource("jdbc:oracle:thin:@127.0.0.1:1521/orcl", "test_dataload", "123456", DbType.ORACLE, null);
        sourceMeta.setDataSource(dataSource);
        applicationValue.setSource(sourceMeta);
        DataSourceMeta   tagertMeta = new DataSourceMeta();
        tagertMeta.setSchemaName("ccbscf_crm");
        tagertMeta.setDbType(DbType.MYSQL);
//        DataSource tagertSource = dataSourceFactory.createDataSource("jdbc:mysql://127.0.0.1:3306/test?serverTimezone=GMT%2B8", "test", "1234", DbType.MYSQL, null);
        DataSource tagertSource = dataSourceFactory.createDataSource("jdbc:mysql://12.0.216.204:13306/ccbscf_crm", "d1_ccbscf", "5SP1avhy", DbType.MYSQL, null);
        tagertMeta.setDataSource(tagertSource);
        applicationValue.setTagert(tagertMeta);

    }

    @Test
    public void connDatabase() {
        DataSourceFactory dataSourceFactory = DataSourceFactory.getDataSourceFactory();
        DataSource dataSource = dataSourceFactory.createDataSource("jdbc:oracle:thin:@127.0.0.1:1521/orcl", "test_dataload", "123456", DbType.ORACLE, null);
        List<Table> tables = TableMetaGenerator.getTableMetasWithoutColumn(dataSource, "test_dataload", null);
        for (Table table : tables) {
            System.out.println(table.getName());
        }

    }

    @Test
    public void testRecordDtsWorker() throws InterruptedException {
        ForkJoinPool forkJoinPool = ForkJoinUtils.getForkJoinPool();
        DataSourceFactory dataSourceFactory = DataSourceFactory.getDataSourceFactory();
        DataSource dataSource = dataSourceFactory.createDataSource("jdbc:oracle:thin:@127.0.0.1:1521/orcl", "test_dataload", "123456", DbType.ORACLE, null);
        DataSource tagertSource = dataSourceFactory.createDataSource("jdbc:mysql://127.0.0.1:3306/test?serverTimezone=GMT%2B8", "test", "1234", DbType.MYSQL, null);

        List<Table> tables = TableMetaGenerator.getTableMetasWithoutColumn(dataSource, "test_dataload", "t_ci_user");
        //全量参数赋值
        ApplicationValue applicationValue = new ApplicationValue();
        JdbcTemplate jd = new JdbcTemplate(dataSource);
        applicationValue.setJdbcTemplate(jd);
        //源数据库
        DataSourceMeta dataSourceMeta = new DataSourceMeta();
        dataSourceMeta.setDataSource(dataSource);
        dataSourceMeta.setDbType(DbType.ORACLE);
        dataSourceMeta.setSchemaName("TEST_DATALOAD");
        applicationValue.setSource(dataSourceMeta);
        //目标数据库
        DataSourceMeta dataSourceMeta2 = new DataSourceMeta();
        dataSourceMeta2.setDataSource(tagertSource);
        dataSourceMeta2.setSchemaName("test");
        dataSourceMeta2.setDbType(DbType.MYSQL);
        applicationValue.setTagert(dataSourceMeta2);
        tables.parallelStream().forEachOrdered(table -> {
            AbstractRecordWorker worker = DtsWorkerFactory.createWorker(RunMode.FULL, new DataTaskResult(), applicationValue, table);
            worker.setTable(table);
            forkJoinPool.submit(worker);
        });
        Thread.sleep(10000000);
//        if (dataSource instanceof DruidDataSource) {
//            DruidDataSource druidDataSource = (DruidDataSource) dataSource;
//            druidDataSource.close();
//        }
    }

    @Test
    public void testQueryCount() {
        String QUERY_COUNT_SQL = "select count(*) from {0}.{1}";
        DataSourceFactory dataSourceFactory = DataSourceFactory.getDataSourceFactory();
        DataSource dataSource = dataSourceFactory.createDataSource("jdbc:oracle:thin:@127.0.0.1:1521/orcl", "test_dataload", "123456", DbType.ORACLE, null);
        JdbcTemplate jd = new JdbcTemplate(dataSource);
        DataSourceMeta dataSourceMeta = new DataSourceMeta();
        dataSourceMeta.setDataSource(dataSource);
        dataSourceMeta.setSchemaName("TEST_DATALOAD");
        Long aLong = jd.queryForObject(MessageFormat.format(QUERY_COUNT_SQL, dataSourceMeta.getSchemaName(), "T_CI_USER"), Long.class);
        System.out.println("查询数据总量：" + aLong);
    }

    @Test
    public void testTableIsExists() {
        DataSourceFactory dataSourceFactory = DataSourceFactory.getDataSourceFactory();
        DataSource tagertSource = dataSourceFactory.createDataSource("jdbc:mysql://127.0.0.1:3306/test?serverTimezone=GMT%2B8", "test", "1234", DbType.MYSQL, null);
        DataSourceMeta dataSourceMeta = new DataSourceMeta();
        dataSourceMeta.setDataSource(tagertSource);
        dataSourceMeta.setSchemaName("test");
        dataSourceMeta.setDbType(DbType.MYSQL);
        boolean exists = DtsUtils.validateTableNameExist(dataSourceMeta.getDataSource(), dataSourceMeta.getDbType(), dataSourceMeta.getSchemaName(), "t_ci_user");
        System.out.println(exists);
    }

    @Test
    public void testQueryTableValue() {
        String QUERY_ORACLE_PAGE_SQL = "SELECT *" +
                "  FROM (SELECT ROWNUM AS rowno, t.*" +
                "          FROM {0}.{1} t" +
                "         WHERE  ROWNUM <= {3}) table_alias" +
                " WHERE table_alias.rowno >={2}";
        DataSourceFactory dataSourceFactory = DataSourceFactory.getDataSourceFactory();
        DataSource dataSource = dataSourceFactory.createDataSource("jdbc:oracle:thin:@127.0.0.1:1521/orcl", "test_dataload", "123456", DbType.ORACLE, null);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        List<TableValue> tableValue = jdbcTemplate.queryForList(new MessageFormat(QUERY_ORACLE_PAGE_SQL).format(new Object[]{"TEST_DATALOAD", "T_CI_USER", "0", "1106"
        }), TableValue.class);
        System.out.println(tableValue.size());
    }

    @Test
    public void testExtract() {
        String QUERY_ORACLE_PAGE_SQL = "SELECT *" +
                "  FROM (SELECT ROWNUM AS rowno, t.*" +
                "          FROM {0}.{1} t" +
                "         WHERE  ROWNUM <= {3}) table_alias" +
                " WHERE table_alias.rowno >={2}";
        DataSourceFactory dataSourceFactory = DataSourceFactory.getDataSourceFactory();
        DataSource dataSource = dataSourceFactory.createDataSource("jdbc:oracle:thin:@127.0.0.1:1521/orcl", "test_dataload", "123456", DbType.ORACLE, null);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String sql = new MessageFormat(QUERY_ORACLE_PAGE_SQL).format(new Object[]{"TEST_DATALOAD", "T_CI_USER", "0", "20"
        });
        ApplicationValue av = new ApplicationValue();
        DataSourceMeta dsm = new DataSourceMeta();
        dsm.setSchemaName("test_dataload");
        dsm.setSourceEncoding("UTF-8");
        av.setSource(dsm);
        //getTableMeta(final DataSource dataSource, final String schemaName, final String tableName)
        List<Table> tables = TableMetaGenerator.getTableMetasWithoutColumn(dataSource, "test_dataload", "t_ci_user");
        Table table = tables.get(0);
        TableMetaGenerator.buildColumns(dataSource, table);
//        Table table = TableMetaGenerator.getTableMeta(dataSource, "test_dataload", "t_ci_user");
        System.out.println("table 的结构----" + table.toString());
        List<TableValue> tableValues = (List<TableValue>) jdbcTemplate.execute(sql, new PreparedStatementCallback() {

            public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                ResultSet rs = ps.executeQuery();
                List<TableValue> result = Lists.newArrayListWithCapacity(200);
                while (rs.next()) {
                    List<ColumnValue> cms = new ArrayList<ColumnValue>();
                    List<ColumnValue> pks = new ArrayList<ColumnValue>();
                    for (ColumnMeta pk : table.getPrimaryKeys()) {
                        ColumnValue cv = transform(rs, av.getSource().getSourceEncoding(), pk);
                        pks.add(cv);
                    }
                    for (ColumnMeta col : table.getColumns()) {
                        ColumnValue cv = transform(rs, av.getSource().getSourceEncoding(), col);
                        cms.add(cv);
                    }
                    TableValue re = new TableValue(av.getSource().getSchemaName(),
                            table.getName(),
                            pks,
                            cms);
                    result.add(re);
                }
                return result;
            }
        });

        for (TableValue tableValue : tableValues) {
            System.out.println(tableValue.toString());
        }
        System.out.println(tableValues.size());
    }

    public JdbcTemplate dataSourceConfig() {
        DataSourceFactory dataSourceFactory = DataSourceFactory.getDataSourceFactory();
        DataSource dataSource = dataSourceFactory.createDataSource("jdbc:oracle:thin:@127.0.0.1:1521/orcl", "test_dataload", "123456", DbType.ORACLE, null);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        return jdbcTemplate;
    }

    public JdbcTemplate dataSourceConfigMysql() {
        DataSourceFactory dataSourceFactory = DataSourceFactory.getDataSourceFactory();
        DataSource tagertSource = dataSourceFactory.createDataSource("jdbc:mysql://12.0.216.204:13306/ccbscf_crm", "d1_ccbscf", "5SP1avhy", DbType.MYSQL, null);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(tagertSource);
        return jdbcTemplate;
    }

    @Test
    public void testApplier() throws SQLException {
        List<TableValue> tableValues = extract();
        boolean redoOneByOne = false;
        TableSqlUnit sqlUnit = getSqlUnit(tableValues.get(0));
        JdbcTemplate jdbcTemplate = dataSourceConfigMysql();

            final Map<String, Integer> indexs = sqlUnit.applierIndexs;

            try {
                jdbcTemplate.execute("", new PreparedStatementCallback() {

                    public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                        for (TableValue record : tableValues) {
                            // 先加字段，后加主键
                            List<ColumnValue> cvs = record.getColumns();
                            for (ColumnValue cv : cvs) {
                                ps.setObject(getIndex(indexs, cv, false), cv.getValue(), cv.getColumn().getType());
                            }

                            // 添加主键
                            List<ColumnValue> pks = record.getPrimaryKeys();
                            for (ColumnValue pk : pks) {
                                ps.setObject(getIndex(indexs, pk, false), pk.getValue(), pk.getColumn().getType());
                            }
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        return null;
                    }
                });
            } catch (Exception e) {
                // catch the biggest exception,no matter how, rollback it;
                redoOneByOne = true;
                System.out.println(e.getMessage());
                jdbcTemplate.getDataSource().getConnection().close();
            }

            // batch cannot pass the duplicate entry exception,so
            // if executeBatch throw exception,rollback it, and
            // redo it one by one
            if (redoOneByOne) {
                applyOneByOne(jdbcTemplate, tableValues, sqlUnit);
            }
    }

//    protected Integer getIndex(final Map<String, Integer> indexs, ColumnValue cv, boolean notExistReturnNull) {
//        Integer result = indexs.get(cv.getColumn().getName());
//        if (result == null && !notExistReturnNull) {
//            throw new DtsException("not found column[" + cv.getColumn().getName() + "] in record");
//        } else {
//            return result;
//        }
//    }

    /**
     * batch处理支持
     */
    protected void applierByBatch(JdbcTemplate jdbcTemplate, final List<TableValue> batchRecords, AbstractRecordWorker.TableSqlUnit sqlUnit) {
        boolean redoOneByOne = false;
        try {
            final Map<String, Integer> indexs = sqlUnit.applierIndexs;
            jdbcTemplate.execute(sqlUnit.applierSql, new PreparedStatementCallback() {

                public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                    for (TableValue record : batchRecords) {
                        // 先加字段，后加主键
                        List<ColumnValue> cvs = record.getColumns();
                        for (ColumnValue cv : cvs) {
                            ps.setObject(getIndex(indexs, cv,false), cv.getValue(), cv.getColumn().getType());
                        }

                        // 添加主键
                        List<ColumnValue> pks = record.getPrimaryKeys();
                        for (ColumnValue pk : pks) {
                            ps.setObject(getIndex(indexs, pk,false), pk.getValue(), pk.getColumn().getType());
                        }

                        ps.addBatch();
                    }

                    ps.executeBatch();
                    return null;
                }
            });
        } catch (Exception e) {
            // catch the biggest exception,no matter how, rollback it;
            redoOneByOne = true;
            // conn.rollback();
        }

        // batch cannot pass the duplicate entry exception,so
        // if executeBatch throw exception,rollback it, and
        // redo it one by one
        if (redoOneByOne) {
            applyOneByOne(jdbcTemplate, batchRecords, sqlUnit);
        }
    }

    /**
     * 一条条记录串行处理
     */
    protected void applyOneByOne(JdbcTemplate jdbcTemplate, final List<TableValue> records, AbstractRecordWorker.TableSqlUnit sqlUnit) {
        final Map<String, Integer> indexs = sqlUnit.applierIndexs;
        jdbcTemplate.execute(sqlUnit.applierSql, new PreparedStatementCallback() {

            public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                for (TableValue record : records) {
                    List<ColumnValue> pks = record.getPrimaryKeys();
                    // 先加字段，后加主键
                    List<ColumnValue> cvs = record.getColumns();
                    for (ColumnValue cv : cvs) {
                        ps.setObject(getIndex(indexs, cv,false), cv.getValue(), cv.getColumn().getType());
                    }

                    // 添加主键
                    for (ColumnValue pk : pks) {
                        ps.setObject(getIndex(indexs, pk,false), pk.getValue(), pk.getColumn().getType());
                    }

                    try {
                        ps.execute();
                    } catch (SQLException e) {
//                        if (context.isSkipApplierException()) {
//                            logger.error("skiped record data : " + record.toString(), e);
//                        } else {
//                            if (e.getMessage().contains("Duplicate entry")
//                                    || e.getMessage().startsWith("ORA-00001: 违反唯一约束条件")) {
//                                logger.error("skiped record data ,maybe transfer before,just continue:"
//                                        + record.toString());
//                            } else {
//                                throw new SQLException("failed Record Data : " + record.toString(), e);
//                            }
//                        }
                    }
                }

                return null;
            }

        });
    }

    public List<TableValue> extract() {
        String QUERY_ORACLE_PAGE_SQL = "SELECT *" +
                "  FROM (SELECT ROWNUM AS rowno, t.*" +
                "          FROM {0}.{1} t" +
                "         WHERE  ROWNUM <= {3}) table_alias" +
                " WHERE table_alias.rowno >={2}";
        DataSourceFactory dataSourceFactory = DataSourceFactory.getDataSourceFactory();
        DataSource dataSource = dataSourceFactory.createDataSource("jdbc:oracle:thin:@127.0.0.1:1521/orcl", "test_dataload", "123456", DbType.ORACLE, null);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplateApp = jdbcTemplate;
        String sql = new MessageFormat(QUERY_ORACLE_PAGE_SQL).format(new Object[]{"TEST_DATALOAD", "T_CI_USER", "0", "20"
        });
        ApplicationValue av = new ApplicationValue();
        DataSourceMeta dsm = new DataSourceMeta();
        dsm.setSchemaName("test_dataload");
        dsm.setSourceEncoding("UTF-8");
        av.setSource(dsm);
        DataSource tagertSource = dataSourceFactory.createDataSource("jdbc:mysql://12.0.216.204:13306/ccbscf_crm", "d1_ccbscf", "5SP1avhy", DbType.MYSQL, null);
        DataSourceMeta ds = new DataSourceMeta();
        ds.setSchemaName("ccbscf_crm");
        ds.setDataSource(tagertSource);
        av.setTagert(ds);
        //getTableMeta(final DataSource dataSource, final String schemaName, final String tableName)
        List<Table> tables = TableMetaGenerator.getTableMetasWithoutColumn(dataSource, "test_dataload", "t_ci_user");
        Table table = tables.get(0);
        TableMetaGenerator.buildColumns(dataSource, table);
//        Table table = TableMetaGenerator.getTableMeta(dataSource, "test_dataload", "t_ci_user");
        System.out.println("table 的结构----" + table.toString());
        List<TableValue> tableValues = (List<TableValue>) jdbcTemplate.execute(sql, new PreparedStatementCallback() {

            public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                ResultSet rs = ps.executeQuery();
                List<TableValue> result = Lists.newArrayListWithCapacity(200);
                while (rs.next()) {
                    List<ColumnValue> cms = new ArrayList<ColumnValue>();
                    List<ColumnValue> pks = new ArrayList<ColumnValue>();
                    for (ColumnMeta pk : table.getPrimaryKeys()) {
                        ColumnValue cv = transform(rs, av.getSource().getSourceEncoding(), pk);
                        pks.add(cv);
                    }
                    for (ColumnMeta col : table.getColumns()) {
                        ColumnValue cv = transform(rs, av.getSource().getSourceEncoding(), col);
                        cms.add(cv);
                    }
                    TableValue re = new TableValue(av.getTagert().getSchemaName(),
                            table.getName(),
                            pks,
                            cms);
                    result.add(re);
                }
                return result;
            }
        });
        return tableValues;
    }

    protected Integer getIndex(final Map<String, Integer> indexs, ColumnValue cv, boolean notExistReturnNull) {
        Integer result = indexs.get(cv.getColumn().getName());
        if (result == null && !notExistReturnNull) {
            throw new DtsException("not found column[" + cv.getColumn().getName() + "] in record");
        } else {
            return result;
        }
    }

    /**
     * Get primary key field information
     */
    protected String[] getPrimaryNames(TableValue record) {
        String[] result = new String[record.getPrimaryKeys().size()];
        int i = 0;
        for (ColumnValue col : record.getPrimaryKeys()) {
            result[i++] = col.getColumn().getName();
        }
        return result;
    }


    /**
     * Get normal column field information
     */
    protected String[] getColumnNames(TableValue record) {
        String[] result = new String[record.getColumns().size()];
        int i = 0;
        for (ColumnValue col : record.getColumns()) {
            result[i++] = col.getColumn().getName();
        }
        return result;
    }

    /**
     * 检查下是否存在必要的字段
     */
    protected void checkColumns(Table meta, Map<String, Integer> indexs) {
        Set<String> idx = new HashSet<String>();
        for (ColumnMeta column : meta.getColumns()) {
            idx.add(column.getName());
        }

        for (ColumnMeta column : meta.getPrimaryKeys()) {
            idx.add(column.getName());
        }

        for (String key : indexs.keySet()) {
            if(!StringUtils.equals(key,"SEQUENCE_NO"))
            if (!idx.contains(key)) {
                throw new DtsException("not found column[" + key + "] in target db");
            }
        }
    }

    protected TableSqlUnit getSqlUnit(TableValue record) {
        List<String> names = Arrays.asList(record.getSchemaName(), record.getTableName());
        TableSqlUnit sqlUnit = applierSqlCache.get(names);
        DbType dbType = applicationValue.getTagert().getDbType();
        if (sqlUnit == null) {
            synchronized (names) {
                sqlUnit = applierSqlCache.get(names);
                if (sqlUnit == null) { // double-check
                    sqlUnit = new TableSqlUnit();
                    String applierSql = null;
                    Table meta = TableMetaGenerator.getTableMeta(applicationValue.getTagert().getDataSource(),
                        applicationValue.isIgnoreSchema() ? null : names.get(0),
                            names.get(1));

                    String[] primaryKeys = getPrimaryNames(record);
                    String[] columns = getColumnNames(record);
                    if (useMerge) {
                        if (dbType == DbType.MYSQL) {
                            applierSql = SqlTemplates.MYSQL.getMergeSql(meta.getSchema(),
                                    meta.getName(),
                                    primaryKeys,
                                    columns,
                                    true);
                        }  else if (dbType == DbType.ORACLE) {
                            applierSql = SqlTemplates.ORACLE.getMergeSql(meta.getSchema(),
                                    meta.getName(),
                                    primaryKeys,
                                    columns);
                        }
                    } else {
                        if (dbType == DbType.MYSQL) {
                            // 如果mysql，全主键时使用insert ignore
                            applierSql = SqlTemplates.MYSQL.getInsertNomalSql(meta.getSchema(),
                                    meta.getName(),
                                    primaryKeys,
                                    columns);
                        } else {
                            applierSql = SqlTemplates.ORACLE.getInsertSql(meta.getSchema(),
                                    meta.getName(),
                                    primaryKeys,
                                    columns);
                        }
                    }

                    int index = 1;
                    Map<String, Integer> indexs = new HashMap<String, Integer>();
                    for (String column : columns) {
                        indexs.put(column, index);
                        index++;
                    }

                    for (String column : primaryKeys) {
                        indexs.put(column, index);
                        index++;
                    }
                    // 检查下是否少了列
                    checkColumns(meta, indexs);

                    sqlUnit.applierSql = applierSql;
                    sqlUnit.applierIndexs = indexs;
                    applierSqlCache.put(names, sqlUnit);
                }
            }
        }

        return sqlUnit;
    }

    public ColumnValue transform(ResultSet rs, String encoding, ColumnMeta col) throws SQLException {
        Object value = null;
        if (col.getType() == Types.DATE) {
            value = rs.getTimestamp(col.getName());
            col = new ColumnMeta(col.getName(), Types.TIMESTAMP);
        } else if (col.getType() == Types.TIMESTAMP) {
            value = rs.getTimestamp(col.getName());
            col = new ColumnMeta(col.getName(), Types.TIMESTAMP);
        } else if (DtsUtils.isCharType(col.getType())) {
            // byte[] bytes = rs.getBytes(col.getName());
            // if (bytes == null) {
            // value = rs.getObject(col.getName());
            // } else {
            // try {
            // value = new String(bytes, encoding);
            // } catch (UnsupportedEncodingException e) {
            // throw new YuGongException("codec error!!", e);
            // }
            // }
            value = rs.getString(col.getName());
        } else if (DtsUtils.isClobType(col.getType())) {
            // Clob c = rs.getClob(col.getName());
            // if (c == null) {
            // value = rs.getObject(col.getName());
            // } else {
            // InputStream is = c.getAsciiStream();
            // byte[] bb = new byte[(int) c.length()];
            // try {
            // is.read(bb);
            // } catch (IOException e) {
            // throw new SQLException("read from clob error,column:" +
            // col.getName(), e);
            // }
            //
            // try {
            // value = new String(bb, encoding);
            // } catch (UnsupportedEncodingException e) {
            // throw new RuntimeException("codec error!!", e);
            // }
            // }
            value = rs.getString(col.getName());
        } else if (DtsUtils.isBlobType(col.getType())) {
            value = rs.getBytes(col.getName());
        } else {
            value = rs.getObject(col.getName());
        }
        return new ColumnValue(col.clone(), value);
    }


}
