package com.dts.core.common.meta;

import com.dts.core.common.v1.DbType;
import lombok.Getter;
import lombok.Setter;

import javax.sql.DataSource;

public class DataSourceMeta {
    @Setter
    @Getter
    private DbType dbType;
    @Setter
    @Getter
    private DataSource dataSource;
    @Setter
    @Getter
    private String schemaName;
    @Getter
    @Setter
    private String sourceEncoding;
}
