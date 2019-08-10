package com.dts.core;

import com.dts.core.util.ForkJoinUtils;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

public class TestForkJoinWorker {
    private static final long THREAD_SIZE = 2;

    //测试任务调度
    @Test
    public void testTaskSche() throws Exception {
        ForkJoinPool forkJoinPool = ForkJoinUtils.getForkJoinPool();
        List<String> list = new ArrayList(Arrays.asList("1", "2", "3", "4", "5", "6", "10"));
        List<String> list2 = new ArrayList(Arrays.asList("11", "22", "33", "44"));
        List<String> list3 = new ArrayList(Arrays.asList("555", "666", "777", "888"));
        List<List<String>> listList = new ArrayList(Arrays.asList(list, list2, list3));
        int sumr = 0;
        for (List str : listList) {
            ForkJoinTask submit = forkJoinPool.submit(new TaskWorker(str));
            Integer o = (Integer) submit.get();
            System.out.println("打印出数据：  " + o + " 当期时间：" + LocalDateTime.now());
            sumr += o;
        }
        System.out.println("sum: " + sumr);
    }

    class TaskWorker<T, V extends Integer> extends RecursiveTask<V> {

        private T t = null;

        @Override
        protected V compute() {
            Integer sum = 0;
            if (Objects.nonNull(t) && t instanceof List) {
                ArrayList<String> strings = (ArrayList<String>) t;
                if (strings.size() <= 2) {
                    for (String str : strings) {
                        sum += Integer.parseInt(str);
                    }
                } else {
                    ArrayList<String> ts = new ArrayList<>(strings.subList(0, 2));
                    ArrayList<String> ts2 = new ArrayList<>(strings.subList(2, strings.size()));
                    TaskWorker t = new TaskWorker(ts);
                    TaskWorker t2 = new TaskWorker(ts2);
                    t.fork();
                    t2.fork();
                    Integer int1 = (Integer) t.join();
                    Integer int2 = (Integer) t2.join();
                    sum += int1;
                    sum += int2;
                }

            }
            return (V) sum;
        }

        TaskWorker(T v) {
            t = v;
        }

    }
}
