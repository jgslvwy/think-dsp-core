package com.dts.core.model;

import com.dts.core.common.v1.RunMode;
import com.dts.core.common.v1.RunState;
import lombok.*;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
public abstract class DataTask {
    @Setter
    @Getter
    private String taskId;
    @Setter
    @Getter
    private RunMode runMode;
    @Setter
    @Getter
    private RunState runState;
    @Getter
    @Setter
    private List<String> tables;
    @Setter
    @Getter
    private String sourceUri;
    @Setter
    @Getter
    private String sourceUserName;
    //TODO 补充源库的schema
    @Setter
    @Getter
    private String sourceSchema;
    @Setter
    @Getter
    private String sourcePassword;
    @Setter
    @Getter
    private String targetUri;
    @Setter
    @Getter
    private String targetSchema;
    @Setter
    @Getter
    private String targetUserName;
    @Setter
    @Getter
    private String targetPassword;
}

