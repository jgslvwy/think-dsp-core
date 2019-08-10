package com.dts.core.model;

import com.dts.core.common.meta.DataSourceMeta;
import lombok.*;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.concurrent.ThreadPoolExecutor;

@AllArgsConstructor
@NoArgsConstructor
public class ApplicationValue {
    //迁移数据的峰值
    @Getter
    @Setter
    private int maxOnceCrawNum = 200;
    //暂时封装一个
    @Getter
    @Setter
    private JdbcTemplate jdbcTemplate;
    //TODO 数据库包装类，包含数据源库链接和数据库类型
    @Setter
    @Getter
    private DataSourceMeta source;
    //目标库链接
    @Getter
    @Setter
    private DataSourceMeta tagert;
    @Getter
    @Setter
    private boolean ignoreSchema = Boolean.FALSE;
    //TODO 用队列实现创建表的过程--如果执行完成后就返回，全局变量，可能导致线程阻塞
//    public BlockingDeque
    private ThreadPoolExecutor threadPoolExecutor;
}
