package com.dts.core.engine;

import com.dts.core.common.meta.Table;
import com.dts.core.common.meta.TableMetaGenerator;
import com.dts.core.common.v1.DbType;
import com.dts.core.exception.DtsException;
import com.dts.core.model.ApplicationValue;
import com.dts.core.model.DataTask;
import com.dts.core.model.DataTaskResult;
import com.dts.core.util.DataSourceFactory;
import com.dts.core.util.DtsWorkerFactory;
import com.dts.core.util.ForkJoinUtils;
import com.dts.core.worker.AbstractRecordWorker;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import javax.sql.DataSource;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.springframework.util.CollectionUtils;

@NoArgsConstructor
@AllArgsConstructor
@ToString
public class DtsCoreEngine<T> implements Callable {

    static volatile DataTaskResult dataTaskResult = new DataTaskResult();

    private DataTask dataTask;

    @Override
    public Object call() {
        ForkJoinPool forkJoinPool = ForkJoinUtils.getForkJoinPool();
        DataSourceFactory dataSourceFactory = DataSourceFactory.getDataSourceFactory();
        if (dataTask == null) {
            throw new DtsException("dataTask is null");
        }
        ApplicationValue value = new ApplicationValue();
        DataSource dataSource = dataSourceFactory.createDataSource(dataTask.getSourceUri(), dataTask.getSourceUserName(), dataTask.getSourcePassword(), DbType.ORACLE, null);
        List<Table> tableList;
        if (CollectionUtils.isEmpty(dataTask.getTables())) {
            tableList = TableMetaGenerator.getTableMetasWithoutColumn(dataSource, dataTask.getSourceSchema(), null);
        } else {
            //TODO 根据jdbc连接查询所有表结构，增量表之间以“,”分割，拆分之后，按照查询部分表
            tableList = TableMetaGenerator.getTableMetasWithoutColumn(dataSource, dataTask.getSourceSchema(), null);
        }
        AbstractRecordWorker<Table, DataTaskResult> dtsWorker = DtsWorkerFactory.createWorker(dataTask.getRunMode(), dataTaskResult, value, null);
        tableList.parallelStream().forEachOrdered(table -> {
            //setup table column structure infos
            TableMetaGenerator.buildColumns(dataSource, table);
            dtsWorker.setTable(table);
            forkJoinPool.submit(dtsWorker);
        });
        return dataTaskResult;
    }
}

