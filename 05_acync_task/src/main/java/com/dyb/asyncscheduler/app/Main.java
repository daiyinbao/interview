/**
 * @author dai_yin_bao
 * @date 2026/3/4 11:32
 * @file Main.java
 */
package com.dyb.asyncscheduler.app;

import com.dyb.asyncscheduler.queue.InMemoryTaskQueue;
import com.dyb.asyncscheduler.queue.TaskQueue;
import com.dyb.asyncscheduler.store.InMemoryTaskStore;
import com.dyb.asyncscheduler.store.TaskStore;
import com.dyb.asyncscheduler.task.*;
import com.dyb.asyncscheduler.worker.Worker;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public final class Main {
    public static void main(String[] args) throws Exception {
        TaskStore store = new InMemoryTaskStore();
        TaskQueue queue = new InMemoryTaskQueue(64);
        HandlerRegistry handlerRegistry = new HandlerRegistry();

        //创建线程池，等待queue任务
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);

        //worker线程开启，一直监听queue
        Worker worker = new Worker("worker-1", store, queue, executor, 5000L,handlerRegistry);
        Thread workerThread = new Thread(worker, "worker-1");
        workerThread.start();

        // handler 1: 永远成功
        handlerRegistry.register("TYPE_OK", ctx -> TaskResult.ok());

        // handler 2: 同一个 taskId 第一次失败（可重试），第二次成功
        ConcurrentHashMap<String, Integer> seen = new ConcurrentHashMap<>();
        handlerRegistry.register("TYPE_FAIL_ONCE", ctx -> {
            int c = seen.merge(ctx.taskId(), 1, Integer::sum);
            if (c == 1) {
                return TaskResult.retryableFailure("E_TEMP", "fail_once");
            }
            return TaskResult.ok();
        });


        int n = 10;
        List<String> ids = new ArrayList<>(n);
        // 提交任务（Step 2 仍然是“写库 + 直接入 ready 队列”，Step 5 才切换为 scheduler scan-store）
        for (int i = 0; i < 1; i++) {
            String id = UUID.randomUUID().toString();
            ids.add(id);
            Task t = new Task(id, "TYPE_OK", "{}", "idem-" + id, "shard-0", 1, 10_000L, RetryPolicy.fixed(200L));
            store.insert(t);
            queue.offer(id);
        }
        for (int i = 0; i < 1; i++) {
            String id = UUID.randomUUID().toString();
            ids.add(id);
            Task t = new Task(id, "TYPE_FAIL_ONCE", "{}", "idem-" + id, "shard-0", 1, 10_000L, RetryPolicy.fixed(200L));
            store.insert(t);
            queue.offer(id);
        }

        // Step 2 不引入 DelayQueue：用一个小循环把到期的 RETRY 任务手动 re-offer 回 ready
        long deadline = System.currentTimeMillis() + 8000L;
        while (System.currentTimeMillis() < deadline) {
            boolean allDone = true;

            long now = System.currentTimeMillis();
            for (String id : ids) {
                var t = store.get(id).orElseThrow();

                if (t.state == TaskState.RETRY && t.nextRunAtEpochMs <= now) {
                    queue.offer(id);
                }

                if (!(t.state == TaskState.SUCCESS || t.state == TaskState.DEAD)) {
                    allDone = false;
                }
            }

            if (allDone) break;
            Thread.sleep(20L);
        }

        for (String id : ids) {
            System.out.println(id + " => " + store.get(id).orElseThrow(() -> new IllegalStateException("task missing: " + id)).state);
        }

        worker.stopGracefully();
        workerThread.interrupt();
        workerThread.join(1000L);

        executor.shutdownNow();
    }
}
