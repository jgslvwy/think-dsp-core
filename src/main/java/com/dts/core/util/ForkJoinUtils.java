package com.dts.core.util;

import org.springframework.stereotype.Component;

import java.util.concurrent.ForkJoinPool;

@Component
public class ForkJoinUtils {
    public static ForkJoinPool getForkJoinPool() {
        return ForkJoinPool.commonPool();
    }
}
