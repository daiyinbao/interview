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
    boolean offer(String taskId);

    String take() throws InterruptedException;

    int size();

    int capacity();
}
