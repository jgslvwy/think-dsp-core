package com.dts.core.worker;

import com.dts.core.common.meta.*;
import com.dts.core.common.v1.RunMode;
import com.dts.core.exception.DtsException;
import com.dts.core.model.ApplicationValue;
import com.dts.core.model.DataTaskResult;
import com.dts.core.util.DtsUtils;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.FutureTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FullRecordWorker<T, V> extends AbstractRecordWorker<T, V> {
    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Getter
    @Setter
    private long start;
    @Getter
    @Setter
    private long end;
    // 全局变量，用于工作窃取算法的判断，如果有该标识就不用再次执行迁移过程，判断是否进行迁移，因为volatile非原子性，所以考虑加锁解决这个问题
    volatile boolean NOT_TRANSFORM_FLAG = true;
    // 刚开始考虑用semaphore，最后考虑性能，用可重入锁进行解决，增加锁，判断是否进行加锁
    volatile Lock LOCK = new ReentrantLock();
    private static String QUERY_COUNT_SQL = "select count(*) from {0}.{1}";

    private static String QUERY_ORACLE_PAGE_SQL = "SELECT *" + "  FROM (SELECT ROWNUM AS rowno, t.*"
            + "          FROM {0}.{1} t" + "         WHERE  ROWNUM <= {3}) table_alias"
            + " WHERE table_alias.rowno >={2}";

    public FullRecordWorker(RunMode runMode, DataTaskResult dataTaskResult, ApplicationValue value, T table) {
        this.runMode = runMode;
        this.dataTaskResult = dataTaskResult;
        this.value = value;
        this.table = table;
    }

    public FullRecordWorker(long start, long end) {
        this.start = start;
        this.end = end;
    }

    @Override
    protected V compute() {
        LOG.info("this current thread name:" + Thread.currentThread().getName());
        boolean continueFlag = true;
        if (Objects.isNull(table)) {
            LOG.error("this data is empty");
            throw new DtsException("this data is empty");
        }
        if (Objects.isNull(value)) {
            LOG.error("this applicationValue is null");
            throw new DtsException("this applicationValue is null");
        }
        // v1
        Table t;
        if (table instanceof Table) {
            t = (Table) table;
        } else {
            throw new DtsException("not support Exception");
        }
        JdbcTemplate jdbcTemplate = value.getJdbcTemplate();
        DataSourceMeta sourceMeta = value.getSource();
        DataSourceMeta tagertMeta = value.getTagert();
        if (Objects.isNull(sourceMeta)) {
            LOG.error("source datasource  is null,will be shutdown");
        }
        if (Objects.isNull(tagertMeta)) {
            LOG.error("tagert datasource  is null,will be shutdown");
        }
        // jdbcTemplate.setDataSource(sourceMeta.getDataSource());
        if (NOT_TRANSFORM_FLAG) {
            LOCK.lock();
            if (!DtsUtils.validateTableNameExist(tagertMeta.getDataSource(), tagertMeta.getDbType(),
                    tagertMeta.getSchemaName(), t.getName())) {
                continueFlag = false;
                NOT_TRANSFORM_FLAG = false;
                TableStructureWorker tableStructureWorker = new TableStructureWorker();
                FutureTask task = null;
                try {
                    task = (FutureTask) tableStructureWorker.call();
                } catch (Exception e) {
                    LOCK.unlock();
                    // 开启内部线程的服务
                    tableStructureWorker = null;
                    // 取消该任务
                    this.cancel(continueFlag);
                    LOG.error("{} table tranform failed", t.getName(), e);
                    throw new DtsException("table structure tranform failed");
                }
            }
            Long aLong = jdbcTemplate.queryForObject(
                    MessageFormat.format(QUERY_COUNT_SQL, sourceMeta.getSchemaName(), t.getName()), Long.class);
            if (aLong > 0) {
                this.start = aLong;
                this.end = end;
            } else {
                LOCK.unlock();
                this.cancel(continueFlag);
                throw new DtsException("table data is null");
            }
            LOCK.unlock();
        }
        while (continueFlag) {
            int maxOnceCrawNum = value.getMaxOnceCrawNum();
            if ((end - start) <= maxOnceCrawNum) {
                LOG.info("END:[{}],start:[{}]", end, start);
                // 此处会存在两段逻辑，首先进行收集，然后再处理
                // List<TableValue> tableValue =
                // jdbcTemplate.queryForList(MessageFormat.format(QUERY_ORACLE_PAGE_SQL,
                // sourceMeta.getSchemaName(), t.getName(), String.valueOf(end),
                // String.valueOf(start)), TableValue.class);
                // LOG.info("this-current-thread-name.size:{}",
                // CollectionUtils.isEmpty(tableValue) ? 0 : tableValue.size());
                List<TableValue> tables = extract(MessageFormat.format(QUERY_ORACLE_PAGE_SQL,
                        sourceMeta.getSchemaName(), t.getName(), String.valueOf(end), String.valueOf(start)),
                        jdbcTemplate);
                // TODO 执行插入操作

            } else {
                // 需要复制整体属性到新的内容中，否则数据丢失，可能导致迁移失败
                for (int i = 0; i < (end - start) / maxOnceCrawNum; i++) {
                    FullRecordWorker fullWorker = new FullRecordWorker(start + maxOnceCrawNum * i,
                            start + maxOnceCrawNum * (i + 1));
                    BeanUtils.copyProperties(this, fullWorker, "start", "end");
                    fullWorker.fork();
                }
            }
        }
        return (V) dataTaskResult;
    }

    @Override
    public DataTaskResult process() {
        return null;
    }

    private List<TableValue> extract(String sql, JdbcTemplate jdbcTemplate) {
        return (List<TableValue>) jdbcTemplate.execute(sql, new PreparedStatementCallback() {
            public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                Table t = null;
                if (table instanceof Table) {
                    t = (Table) table;
                } else {
                    throw new DtsException("not support Exception");
                }
                ResultSet rs = ps.executeQuery();
                List<TableValue> result = Lists.newArrayListWithCapacity(200);
                while (rs.next()) {
                    List<ColumnValue> cms = new ArrayList<ColumnValue>();
                    List<ColumnValue> pks = new ArrayList<ColumnValue>();
                    for (ColumnMeta pk : t.getPrimaryKeys()) {
                        ColumnValue cv = transform(rs, value.getSource().getSourceEncoding(), pk);
                        pks.add(cv);
                    }
                    for (ColumnMeta col : t.getColumns()) {
                        ColumnValue cv = transform(rs, value.getSource().getSourceEncoding(), col);
                        cms.add(cv);
                    }
                    TableValue re = new TableValue(value.getSource().getSchemaName(), t.getName(), pks, cms);
                    result.add(re);
                }
                return result;
            }
        });
    }

    @Override
    public void applier(List<TableValue> lists) {

    }
}
