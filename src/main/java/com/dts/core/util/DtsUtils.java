package com.dts.core.util;

import com.dts.core.common.meta.ColumnMeta;
import com.dts.core.common.v1.DbType;
import com.dts.core.exception.DtsException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.List;

public class DtsUtils {
    private static final String MYSQL_TABLE_EXISTS = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA={0} AND TABLE_NAME={1}";

    private static final String ORACLE_TABLE_EXISTS = "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = {0} AND TABLE_NAME = {1}";

    public static boolean isEmpty(Collection collection) {
        return collection == null || collection.size() == 0;
    }

    public static boolean isNotEmpty(Collection collection) {
        return collection != null && collection.size() != 0;
    }

    /**
     * 返回字段名字的数组
     *
     * @param columns
     * @return
     */
    public static String[] getColumnNameArray(List<ColumnMeta> columns) {
        if (columns == null || columns.size() == 0) {
            return new String[]{};
        }

        String[] result = new String[columns.size()];
        int i = 0;
        for (ColumnMeta column : columns) {
            result[i++] = column.getName();
        }

        return result;
    }

    /**
     * 根据DataSource判断一下数据库类型
     *
     * @param dataSource
     * @return
     */
    public static DbType judgeDbType(DataSource dataSource) {
        final JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        return (DbType) jdbcTemplate.execute(new ConnectionCallback() {

            public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                DatabaseMetaData meta = c.getMetaData();
                String databaseName = meta.getDatabaseProductName();
                String version = meta.getDatabaseProductVersion();
                if (StringUtils.startsWithIgnoreCase(databaseName, "oracle")) {
                    return DbType.ORACLE;
                } else if (StringUtils.startsWithIgnoreCase(databaseName, "mysql")) {
                    return DbType.MYSQL;
                } else {
                    throw new DtsException("unknow database type " + databaseName);
                }
            }
        });

    }

    /**
     * Convert a column name with underscores to the corresponding class name
     * using "camel case". A name like "customer_number" would match a
     * "CustomerNumber" class name.
     *
     * @param name the column name to be converted
     * @return the name using "camel case"
     */
    public static String toPascalCase(String name) {
        StringBuilder result = new StringBuilder();
        boolean needUpper = false;
        if (StringUtils.isNotEmpty(name)) {
            result.append(Character.toUpperCase(name.charAt(0)));
            for (int i = 1; i < name.length(); i++) {
                String s = String.valueOf(name.charAt(i));
                if (s.equals("_")) {
                    needUpper = true;
                } else {
                    if (needUpper) {
                        result.append(s.toUpperCase());
                        needUpper = false;
                    } else {
                        result.append(s.toLowerCase());
                    }
                }
            }
        }

        return result.toString();
    }

    public static boolean isCharType(int sqlType) {
        if (sqlType == Types.CHAR || sqlType == Types.VARCHAR || sqlType == Types.NCHAR || sqlType == Types.NVARCHAR) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isClobType(int sqlType) {
        if (sqlType == Types.CLOB || sqlType == Types.LONGVARCHAR || sqlType == Types.NCLOB
                || sqlType == Types.LONGNVARCHAR) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isBlobType(int sqlType) {
        if (sqlType == Types.BLOB || sqlType == Types.BINARY || sqlType == Types.VARBINARY
                || sqlType == Types.LONGVARBINARY) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isNumber(int sqlType) {
        if (sqlType == Types.TINYINT || sqlType == Types.SMALLINT || sqlType == Types.INTEGER
                || sqlType == Types.BIGINT || sqlType == Types.NUMERIC || sqlType == Types.DECIMAL) {
            return true;
        } else {
            return false;
        }
    }

    public static Object encoding(Object source, int sqlType, String sourceEncoding, String targetEncoding) {
        switch (sqlType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
                if (source instanceof String) {
                    String str = (String) source;
                    if (false == StringUtils.isEmpty(str)) {
                        if (false == StringUtils.equalsIgnoreCase(sourceEncoding, targetEncoding)) {
                            try {
                                byte[] bytes = null;
                                if (StringUtils.isBlank(sourceEncoding)) {
                                    bytes = str.getBytes();
                                } else {
                                    bytes = str.getBytes(sourceEncoding);
                                }

                                if (StringUtils.isBlank(targetEncoding)) {
                                    return new String(bytes);
                                } else {
                                    return new String(bytes, targetEncoding);
                                }
                            } catch (UnsupportedEncodingException e) {
                                throw new IllegalArgumentException(e.getMessage(), e);
                            }
                        }
                    }
                }
        }

        return source;
    }

    public static boolean validateTableNameExist(final DataSource dataSource, final DbType dbType, final String tagSchemaName, final String tableName) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String sql = dbType.isOracle() ? ORACLE_TABLE_EXISTS : (dbType.isMysql() ? MYSQL_TABLE_EXISTS : "");
        sql = MessageFormat.format(sql, addQuotes(tagSchemaName), addQuotes(tableName));
        Long integer = jdbcTemplate.queryForObject(sql, Long.class);
        System.out.println("long count " + integer);
        return (integer > 0);
    }

    public static String addQuotes(String str) {
        return "'" + str + "'";
    }

}
