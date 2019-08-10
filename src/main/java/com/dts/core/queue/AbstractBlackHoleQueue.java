package com.dts.core.queue;

import java.util.AbstractQueue;
import java.util.Iterator;

/**
 * 收集的储藏队列，用于线程收集的表结构
 */
public  class AbstractBlackHoleQueue extends AbstractQueue {

    @Override public Iterator iterator() {
        return null;
    }

    @Override public int size() {
        return 0;
    }

    @Override public boolean offer(Object o) {
        return false;
    }

    @Override public Object poll() {
        return null;
    }

    @Override public Object peek() {
        return null;
    }
}
