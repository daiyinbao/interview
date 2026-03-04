/**
 * @author dai_yin_bao
 * @date 2026/3/4 11:06
 * @file Worker.java
 */
package com.dyb.asyncscheduler.worker;

import com.dyb.asyncscheduler.queue.TaskQueue;
import com.dyb.asyncscheduler.store.TaskStore;
import com.dyb.asyncscheduler.util.DebugLog;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * Worker 是“任务消费主循环”，负责从 ReadyQueue 取任务 → 抢租约 → 提交给线程池执行
 */
public final class Worker implements Runnable{


    private final String workerId;
    private final TaskStore store;
    private final TaskQueue queue;
    private final Executor executor;

    private final long leaseTtlMs;

    private volatile boolean stopped;
    private volatile WorkerState state = WorkerState.IDLE;

    public Worker(String workerId, TaskStore store, TaskQueue queue, Executor executor, long leaseTtlMs) {
        this.workerId = Objects.requireNonNull(workerId, "workerId");
        this.store = Objects.requireNonNull(store, "store");
        this.queue = Objects.requireNonNull(queue, "queue");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.leaseTtlMs = leaseTtlMs;
    }

    public WorkerState state() {
        return state;
    }

    public void stopGracefully() {
        stopped = true;
        state = WorkerState.DRAINING;
    }

    @Override
    public void run() {
        DebugLog.log("Worker start workerId=%s leaseTtlMs=%d", workerId, leaseTtlMs);
        while (!stopped && !Thread.currentThread().isInterrupted()) {
            try {
                state = WorkerState.IDLE;
                String taskId = queue.take();
                DebugLog.log("Worker take taskId=%s queueSize=%d", taskId, queue.size());

                long now = System.currentTimeMillis();
                state = WorkerState.RESERVED;
                boolean leased = store.tryLease(taskId, workerId, now, leaseTtlMs);
                DebugLog.log("Worker tryLease taskId=%s leased=%s", taskId, leased);
                if (!leased) {
                    state = WorkerState.BACKOFF;
                    Thread.sleep(10L);
                    continue;
                }

                state = WorkerState.SUBMITTED;
                DebugLog.log("Worker submit TaskRunner taskId=%s", taskId);
                executor.execute(new TaskRunner(taskId, store, workerId));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (RejectedExecutionException ree) {
                // Step 7 会把这里升级成“拒绝闭环”；Step 1 先保证 lease 不悬挂
                state = WorkerState.BACKOFF;
                DebugLog.log("Worker rejected by executor; backoff");
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } catch (Exception ex) {
                state = WorkerState.UNHEALTHY;
                DebugLog.log("Worker exception=%s", ex.toString());
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        state = WorkerState.STOPPED;
        DebugLog.log("Worker stopped workerId=%s", workerId);
    }
}
