/**
 * @author dai_yin_bao
 * @date 2026/3/4 11:06
 * @file Worker.java
 */
package com.dyb.asyncscheduler.worker;

import com.dyb.asyncscheduler.queue.TaskQueue;
import com.dyb.asyncscheduler.store.TaskStore;
import com.dyb.asyncscheduler.task.HandlerRegistry;
import com.dyb.asyncscheduler.util.DebugLog;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Worker 是“任务消费主循环”，负责从 ReadyQueue 取任务 → 抢租约 → 提交给线程池执行
 */
public final class Worker implements Runnable {

    //标识那个worker在执行任务
    private final String workerId;
    //持久化任务状态
    private final TaskStore store;
    private final TaskQueue queue;
    private final Executor executor;
    //任务注册中心
    private final HandlerRegistry registry;

    //有效期权时间
    private final long leaseTtlMs;

    //最大并发数
    private final int maxInflight;
    //当前执行的任务数
    private final AtomicInteger inflight = new AtomicInteger(0);


    private volatile boolean stopped;
    private volatile boolean draining;
    private volatile WorkerState state = WorkerState.IDLE;

    public Worker(String workerId, TaskStore store, TaskQueue queue, Executor executor, long leaseTtlMs, HandlerRegistry registry, int maxInflight) {
        this.workerId = Objects.requireNonNull(workerId, "workerId");
        this.store = Objects.requireNonNull(store, "store");
        this.queue = Objects.requireNonNull(queue, "queue");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.leaseTtlMs = leaseTtlMs;
        this.registry = registry;
        this.maxInflight = maxInflight;
    }

    public WorkerState state() {
        return state;
    }

    public int inflight(){
        return inflight.get();
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
                //pool忙时，不进行后续操作
                if(inflight.get()>=maxInflight){
                    state = WorkerState.BACKOFF;
                    Thread.sleep(10L);
                    continue;
                }
                state = WorkerState.IDLE;
                //一直监听，当queue里面有任务开始执行
                String taskId = queue.take();//只从ready拿
                long now = System.currentTimeMillis();
                //该worker已被占用
                state = WorkerState.RESERVED;
                //获取期权
                boolean leased = store.tryLease(taskId, workerId, now, leaseTtlMs);
                DebugLog.log("Worker tryLease taskId=%s leased=%s", taskId, leased);
                if (!leased) {
                    queue.offer(taskId,now + 50L)
                    state = WorkerState.BACKOFF;
                    Thread.sleep(10L);
                    continue;
                }
                //并发数++
                inflight.incrementAndGet();
                state = WorkerState.SUBMITTED;
                DebugLog.log("Worker submit TaskRunner taskId=%s", taskId);
                //真正开始执行任务
                Runnable wrapper = () -> {
                    try {
                        new TaskRunner(taskId,store,workerId,registry).run();
                    }finally {
                        //任务执行后并发数--
                        inflight.decrementAndGet();
                    }
                };
                try {
                    executor.execute(wrapper);
                } catch (RejectedExecutionException ree) {
                    // Step 4：先保证不悬挂（Step 7 再做 reject 闭环）
                    store.releaseLease(taskId, workerId);
                    queue.offer(taskId, System.currentTimeMillis() + 50L);
                    inflight.decrementAndGet();

                    state = WorkerState.BACKOFF;
                    Thread.sleep(50L);
                }
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

        long deadline = System.currentTimeMillis() + 5000L;
        while (inflight.get() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10L);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        state = WorkerState.STOPPED;
        DebugLog.log("Worker stopped workerId=%s", workerId);
    }
}
