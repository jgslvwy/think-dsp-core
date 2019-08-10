package com.dts.core;

import com.dts.core.exception.DtsException;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/***
 * 为了测试同样的线程场景，做了这个实验
 * 明显实验是成功的，如果增加全局SIGN，
 * <p>lock和sign表示
 */
public class TestThreadMutex {

    Lock LOCK = new ReentrantLock();

    volatile boolean SIGN = true;

    volatile long start = System.currentTimeMillis();

    @Test
    public void TestThreadLock() {
        throw new DtsException("失败");
    }

    @Test
    public void TestThreadVoid() throws Exception {
        System.out.println("接口启动时间：" + System.currentTimeMillis());
//        Thread.currentThread().setDaemon(true);
        ExecutorService exec = Executors.newCachedThreadPool();
//        LOCK.lock();
        try {
            for (int i = 0; i < 10000; i++) {
                if (SIGN) {
                    LOCK.lock();
                    //call直接执行，主线程当前方法在执行，所以必须用start
                    CallTable callTable = new CallTable();
                    callTable.setS(String.valueOf(i));
                    Future submit = exec.submit(callTable);
                    Object v = submit.get();
//                    Object v = callTable.call();

                    if (v instanceof String) {
                        String sv = (String) v;
                        if (sv.contains("2")) {
//                            SIGN = false;
                            //test why i chance throw new Exception is use,but in other not put
//                            throw new Exception("12312312");
                        }
//                    System.out.println("current thread name : " + sv);
                    }
                }
            }
        } finally {
            LOCK.unlock();
            System.out.println("批量执行接口： " + (System.currentTimeMillis() - start));
        }
    }


    static class CallTable implements Callable {
        private String s;

        public void setS(String s) {
            this.s = s;
        }

        @Override
        public Object call() throws Exception {
//            System.out.println("call method return name: " + Thread.currentThread().getName());

//            this deal is bad
//            return Thread.currentThread().getName();
            System.out.println("call method return name: " + s + " time: " + System.currentTimeMillis());
            return s;
        }
    }
}
