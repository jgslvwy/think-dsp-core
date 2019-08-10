package com.dts.core;

public class TestCq {
    private Node node;
    private int size;
    private int length;

    class Node {
        Node last;
        Integer value;
    }

    /**
     * Initialize your data structure here. Set the size of the queue to be k.
     */
    public TestCq(int k) {
        length = k;
    }

    /**
     * Insert an element into the circular queue. Return true if the operation is successful.
     */
    public boolean enQueue(int value) {
        if(length == 0 || length == size){
            return false;
        }
        Node last = new Node();
        last.value = value;
        last.last = null;
        if(null == node){
            node = last;
        } else {
            last.last = node;
        }
        size++;
        node = last;
        return true;
    }

    /**
     * Delete an element from the circular queue. Return true if the operation is successful.
     */
    public boolean deQueue() {
        if(size<=0 || length<=0 || null == node){
            return false;
        }
        Node midd = node.last;
        Node hex = null;
        while(null != midd){
            hex = midd;
            midd = midd.last;
        }
        if(null != hex) {
            //TODO 节点地址找不到
            hex.value = null;
            hex.last = null;
        } else {
            node.value = null;
            node = null;
        }
        size--;
        return true;
    }


    /**
     * Get the front item from the queue.
     */
    public int Front() {
        if(size<=0 || length<=0 || null == node){
            return -1;
        }
        Node midd = node.last;
        Node hex = null;
        while(null != midd){
              hex = new Node();
            hex.value = midd.value;
            midd = midd.last;
        }
        if(null != hex) {
            return hex.value;
        } else {
            return null!=node ? node.value:0;
        }
    }

    /**
     * Get the last item from the queue.
     */
    public int Rear() {
        if(size<=0 || length<=0 || null == node){
            return -1;
        }
        return node.value;
    }

    /**
     * Checks whether the circular queue is empty or not.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Checks whether the circular queue is full or not.
     */
    public boolean isFull() {
        return size == length;
    }

    public static void main(String[] args) {
        TestCq obj = new TestCq(2);
        boolean param_1 = obj.enQueue(4);
//        int param_2 = obj.Rear();
//        int param_3 = obj.Rear();
//        boolean param_4 = obj.deQueue();
//        boolean param_5 = obj.enQueue(5);
//        int param_6 = obj.Rear();
//        obj.deQueue();
//        obj.Front();
//        obj.deQueue();
//        obj.deQueue();
//        obj.deQueue();
        int param_3 = obj.Rear();
        boolean param_2 = obj.enQueue(9);
        obj.Rear();
        obj.deQueue();
        obj.Front();

    }
}
