package com.dts.core;


import java.util.UUID;

public class TestThreadBlocked {
    public static void main(String[] args) {
        for (int i = 0; i < 3; i++) {
            System.out.println(UUID.randomUUID());
        }
        testDeadBlock();
    }

    public static void testDeadBlock() {
        Object o = new Object();
        Object o1 = new Object();
        Runnable thread = new Thread() {
            @Override
            public void run() {
                synchronized (o) {
                    System.out.println("o hashcode:" + o.hashCode());
                    synchronized (o1) {
                        System.out.println("o1 hashcode:" + o1.hashCode());

                    }
                }
            }
        };
        Runnable thread2 = new Thread() {
            @Override
            public void run() {
                synchronized (o1) {
                    System.out.println("obak hashcode:" + o1.hashCode());
                    synchronized (o) {
                        System.out.println("o bak1 hashcode:" + o.hashCode());
                    }
                }
            }
        };
        ((Thread) thread).start();
        ((Thread) thread2).start();

    }
}
