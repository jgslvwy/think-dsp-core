package com.dts.core.worker;

import com.dts.core.common.meta.Table;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.Callable;

/**
 * consider using callable thread for other tasks
 */
public class TableStructureWorker implements Callable {
    @Getter
    @Setter
    private Table table;


    @Override
    public Object call() throws Exception {
        //TODO 表结构迁移---->
        return null;
    }
}
