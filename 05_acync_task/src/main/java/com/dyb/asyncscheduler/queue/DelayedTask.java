/**
 * @author dai_yin_bao
 * @date 2026/3/6 17:08
 * @file DelayedTask.java
 */
package com.dyb.asyncscheduler.queue;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class DelayedTask implements Delayed {
    public final String taskId;
    //执行任务的时间戳(何时执行任务)
    public final long runAtEpochMs;

    public DelayedTask(String taskId, long runAtEpochMs) {
        this.taskId = taskId;
        this.runAtEpochMs = runAtEpochMs;
    }

    /**
     * 返回任务还需要等待多久
     * @param unit
     * @return
     */
    @Override
    public long getDelay(TimeUnit unit) {
        long now = System.currentTimeMillis();
        long delayMs = runAtEpochMs - now;
        return unit.convert(delayMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 决定 DelayQueue 中任务的排序
     * @param o
     * @return
     */
    @Override
    public int compareTo(Delayed o) {
        if (this == o) return 0;
        long d = getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS);
        return (d == 0) ? 0 : (d < 0 ? -1 : 1);
    }
}
