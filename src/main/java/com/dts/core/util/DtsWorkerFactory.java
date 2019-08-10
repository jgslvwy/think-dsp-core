package com.dts.core.util;

import com.dts.core.common.meta.Table;
import com.dts.core.common.v1.RunMode;
import com.dts.core.exception.DtsException;
import com.dts.core.model.ApplicationValue;
import com.dts.core.model.DataTaskResult;
import com.dts.core.worker.AbstractRecordWorker;
import com.dts.core.worker.FullRecordWorker;

import java.util.Objects;

public class DtsWorkerFactory<T> {
    public static AbstractRecordWorker createWorker(RunMode runMode, DataTaskResult dataTaskResult, ApplicationValue value, Table table) {
        if (Objects.equals(runMode, RunMode.FULL)) {
            return new FullRecordWorker(runMode, dataTaskResult, value, table);
        } else if (Objects.equals(runMode, RunMode.INC)) {
            //TODO INC 模式暂时先不开发
            return null;
        } else {
            throw new DtsException("this runmode is incorrect or not support");
        }
    }
}