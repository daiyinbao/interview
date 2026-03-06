/**
 * @author dai_yin_bao
 * @date 2026/3/4 10:19
 * @file InMemoryTaskQueue.java
 */
package com.dyb.asyncscheduler.queue;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * 基于内存的就绪队列实现
 */
public  final class InMemoryTaskQueue implements TaskQueue{
    //有界就绪队列-->底层是环形队列
    private final ArrayBlockingQueue<String> q;


    public InMemoryTaskQueue(int capacity) {
        //初始化就绪队列
        if(capacity<=0) throw new IllegalArgumentException("capacity must be > 0");
        this.q = new ArrayBlockingQueue<>(capacity);
    }


    /**
     * 存放任务id，if queue满了返回false
     * @param taskId
     * @return
     */
    @Override
    public boolean offer(String taskId) {
        Objects.requireNonNull(taskId, "taskId");
        return q.offer(taskId);
    }

    @Override
    public boolean offer(String taskId, long nextRunAtEpochMs) {
        return false;
    }

    @Override
    public boolean offer(String taskId, long nextRunAtEpochMs, long timeoutMs) throws InterruptedException {
        return false;
    }

    /**
     * 从队列中取任务
     * @return
     * @throws InterruptedException
     */
    @Override
    public String take() throws InterruptedException {
        return q.take();
    }

    @Override
    public int readySize() {
        return 0;
    }

    @Override
    public int readyCapacity() {
        return 0;
    }

    @Override
    public int delaySize() {
        return 0;
    }

    @Override
    public void shutdown() {

    }

}
