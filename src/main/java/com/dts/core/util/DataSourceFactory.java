package com.dts.core.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.dts.core.common.v1.DbType;
import com.dts.core.exception.DtsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * FROM YUGONG
 */
public class DataSourceFactory {
    //TODO 初始化连接池配置写死，重点不在这里面，所以不需要细化
    private int initialSize = 100;
    private int minIdle = 100;
    private int maxWait = 100;
    private int maxActive = 100;
    private Logger LOG = LoggerFactory.getLogger(this.getClass());
    private static DataSourceFactory dataSourceFactory;

    public DataSource createDataSource(String url, String userName, String password, DbType dbType, Properties props) {
        //TODO 补充完善连接属性和内容
        try {
            //TODO 如果有机会可以看看连接池的概念，要不然真得被阿里垄断了
            DruidDataSource dataSource = new DruidDataSource();
            dataSource.setUrl(url);
            dataSource.setUsername(userName);
            dataSource.setPassword(password);
            dataSource.setUseUnfairLock(true);
            dataSource.setNotFullTimeoutRetryCount(2);
            dataSource.setInitialSize(initialSize);
            dataSource.setMinIdle(minIdle);
            dataSource.setMaxActive(maxActive);
            dataSource.setMaxWait(maxWait);
            dataSource.setDriverClassName(dbType.getDriver());
            // 动态的参数
            if (props != null && props.size() > 0) {
                for (Map.Entry<Object, Object> entry : props.entrySet()) {
                    dataSource.addConnectionProperty(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
                }
            }

            if (dbType.isOracle()) {
                dataSource.addConnectionProperty("restrictGetTables", "true");
                dataSource.addConnectionProperty("oracle.jdbc.V8Compatible", "true");
                dataSource.setValidationQuery("select 1 from dual");
                dataSource.setExceptionSorter("com.alibaba.druid.pool.vendor.OracleExceptionSorter");
            } else if (dbType.isMysql()) {
                dataSource.addConnectionProperty("useServerPrepStmts", "false");
                dataSource.addConnectionProperty("rewriteBatchedStatements", "true");
                dataSource.addConnectionProperty("allowMultiQueries", "true");
                dataSource.addConnectionProperty("readOnlyPropagatesToServer", "false");
                dataSource.setValidationQuery("select 1");
                dataSource.setExceptionSorter("com.alibaba.druid.pool.vendor.MySqlExceptionSorter");
                dataSource.setValidConnectionCheckerClassName("com.alibaba.druid.pool.vendor.MySqlValidConnectionChecker");
            } else {
                LOG.error("Unknow database type");
            }
            return dataSource;
        } catch (Throwable e) {
            throw new DtsException("create dataSource error!", e);
        }
    }

    public static DataSourceFactory getDataSourceFactory() {
        synchronized (DataSourceFactory.class) {
            if (Objects.isNull(dataSourceFactory)) {
                dataSourceFactory = new DataSourceFactory();
            }
        }
        return dataSourceFactory;
    }
}
