package com.dts.core.worker;

import com.dts.core.common.meta.ColumnMeta;
import com.dts.core.common.meta.ColumnValue;
import com.dts.core.common.meta.TableValue;
import com.dts.core.common.v1.DbType;
import com.dts.core.common.v1.RunMode;
import com.dts.core.model.ApplicationValue;
import com.dts.core.model.DataTaskResult;
import com.dts.core.util.DtsUtils;
import com.google.common.collect.Lists;
import lombok.Data;
import lombok.Setter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RecursiveTask;

public abstract class AbstractRecordWorker<T, V> extends RecursiveTask<V> {
    @Setter
    protected RunMode runMode;
    protected DataTaskResult dataTaskResult;
    protected ApplicationValue value;
    @Setter
    protected T table;
    @Data
    public static class TableSqlUnit {
        public String               applierSql;
        public Map<String, Integer> applierIndexs;
    }

    /**
     * this is data load schedule
     *
     * @return
     */
    protected abstract DataTaskResult process();

    /**
     * this method deal with data transform,support oracle->oracle|oracle->mysql
     * v1 only support this,but contain furture will give mysql->mysql|mysql->oracle
     * type transform
     * <p>this transform DATE to TIMESTAMP
     * <version>from yugong
     */
    protected ColumnValue transform(ResultSet rs, String encoding, ColumnMeta col) throws SQLException {
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

    /**
     * support customize datatype translator
     * this is can deal with the scenes
     */
    protected void translator() {

    }

    /**
     * be responsible for extract data value
     *
     * @return
     */
    protected List<TableValue> extract(JdbcTemplate jdbcTemplate) {
        return Lists.newArrayList();
    }

    /**
     * be responsible for custom data value
     */
    protected void applier(List<TableValue> lists) {
    }

}
