/**
 * @author dai_yin_bao
 * @date 2026/3/4 10:10
 * @file TaskQueue.java
 */
package com.dyb.asyncscheduler.queue;

/**
 * 就绪队列抽象
 */
public interface TaskQueue {

    default boolean offer(String taskId) {
        return offer(taskId, System.currentTimeMillis());
    }

    /**
     * nextRunAt<=now：进入 ready（有界，可能返回 false）
     * nextRunAt>now：进入 delay（等待到期转移到 ready）
     */
    boolean offer(String taskId, long nextRunAtEpochMs);

    /**
     * 带超时版本：常用于 transfer（避免 ready 满时忙等）
     */
    boolean offer(String taskId, long nextRunAtEpochMs, long timeoutMs) throws InterruptedException;

    String take() throws InterruptedException;

    int readySize();

    int readyCapacity();

    int delaySize();

    void shutdown();
}
