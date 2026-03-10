/**
 * @author dai_yin_bao
 * @date 2026/3/4 11:32
 * @file Main.java
 */
package com.dyb.asyncscheduler.app;

import com.dyb.asyncscheduler.queue.InMemoryTaskQueue;
import com.dyb.asyncscheduler.store.InMemoryTaskStore;
import com.dyb.asyncscheduler.store.TaskStore;
import com.dyb.asyncscheduler.task.*;
import com.dyb.asyncscheduler.worker.Worker;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public final class Main {
    public static void main(String[] args) throws Exception {
        TaskStore store = new InMemoryTaskStore();
        InMemoryTaskQueue queue = new InMemoryTaskQueue(1); // 刻意设小，制造背压
        HandlerRegistry registry = new HandlerRegistry();

        registry.register("TYPE_OK", ctx -> TaskResult.ok());

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);

        Worker worker = new Worker("worker-1", store, queue, executor, 5000L, registry,100);
        Thread workerThread = new Thread(worker, "worker-1");
        workerThread.start();

        long now = System.currentTimeMillis();

        // 1) 塞满 ready
        for (int i = 0; i < 3; i++) {
            String id = UUID.randomUUID().toString();
            Task t = new Task(id, "TYPE_OK", "{}", "idem-" + id, "shard-0", 1, 10_000L, RetryPolicy.fixed(200L));
            store.insert(t);
            boolean ok = queue.offer(id, now);
            System.out.println("offer immediate ok=" + ok + " taskId=" + id);
        }

        // 2) 第 4 个立即任务：ready 满，应明确失败（背压信号）
        {
            String id = UUID.randomUUID().toString();
            Task t = new Task(id, "TYPE_OK", "{}", "idem-" + id, "shard-0", 1, 10_000L, RetryPolicy.fixed(200L));
            store.insert(t);
            boolean ok = queue.offer(id, now);
            System.out.println("offer immediate when full ok=" + ok + " (expect false) taskId=" + id);
        }

        // 3) 延迟任务：不会占 ready，到期后 transfer 转移并执行
        String delayId = UUID.randomUUID().toString();
        Task delayTask = new Task(delayId, "TYPE_OK", "{}", "idem-" + delayId, "shard-0", 1, 10_000L, RetryPolicy.fixed(200L));
        store.insert(delayTask);
        boolean delayOffered = queue.offer(delayId, now + 1000L);
        System.out.println("offer delay ok=" + delayOffered + " taskId=" + delayId);

        // 等待最多 5s，看延迟任务是否最终 SUCCESS
        long deadline = System.currentTimeMillis() + 5000L;
        while (System.currentTimeMillis() < deadline) {
            TaskState s = store.get(delayId).orElseThrow().state;
            if (s == TaskState.SUCCESS) break;
            Thread.sleep(20L);
        }

        System.out.println("delay task state=" + store.get(delayId).orElseThrow().state);
        System.out.println("queue.offerRejectCount=" + queue.offerRejectCount() + " requeueCount=" + queue.requeueCount());

        worker.stopGracefully();
        workerThread.interrupt();
        workerThread.join(1000L);

        queue.shutdown();
        executor.shutdownNow();
    }
}
