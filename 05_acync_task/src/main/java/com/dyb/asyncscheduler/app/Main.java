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
import com.dyb.asyncscheduler.task.Task;
import com.dyb.asyncscheduler.task.TaskState;
import com.dyb.asyncscheduler.util.DebugLog;
import com.dyb.asyncscheduler.worker.Worker;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public final class Main {
    public static void main(String[] args) throws Exception {
        TaskStore store = new InMemoryTaskStore();
        TaskQueue queue = new InMemoryTaskQueue(64);

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);

        Worker worker = new Worker("worker-1", store, queue, executor, 5000L);
        Thread workerThread = new Thread(worker, "worker-1");
        workerThread.start();

        int n = 10;
        List<String> ids = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            String id = UUID.randomUUID().toString();
            ids.add(id);

            Task t = new Task(id, "demo", "{}", "idem-" + id, "shard-0");
            store.insert(t);
            DebugLog.log("Main inserted taskId=%s state=%s", id, store.get(id).orElseThrow(() -> new IllegalStateException("task missing: " + id)).state);

            boolean offered = queue.offer(id);
            DebugLog.log("Main offered taskId=%s offered=%s queueSize=%d", id, offered, queue.size());
            if (!offered) {
                throw new IllegalStateException("ready queue full in Step 1");
            }
        }

        long deadline = System.currentTimeMillis() + 5000L;
        long lastWaitLogAt = 0L;
        while (System.currentTimeMillis() < deadline) {
            boolean allDone = true;
            for (String id : ids) {
                TaskState s = store.get(id).orElseThrow(() -> new IllegalStateException("task missing: " + id)).state;
                if (s != TaskState.SUCCESS) {
                    allDone = false;
                    break;
                }
            }
            if (!allDone) {
                long now = System.currentTimeMillis();
                if (now - lastWaitLogAt >= 200L) {
                    lastWaitLogAt = now;
                    DebugLog.log("Main waiting... (not all SUCCESS yet)");
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
