package scheduler;

import java.util.concurrent.Semaphore;

/**
 * Worker线程
 *
 * 说明：
 * 1. Worker负责从TaskQueue中不断take任务。
 * 2. 通过Semaphore进行限流，控制系统最大并发执行任务数量。
 * 3. 任务执行交由TaskScheduler内部的CompletableFuture处理（含超时与重试）。
 */
public class Worker extends Thread {

    // 任务队列，Worker从这里take任务
    private final TaskQueue taskQueue;

    // 调度器引用，用于更新任务状态与完成通知
    private final TaskScheduler scheduler;

    // 信号量限流，控制最大并发执行任务数
    private final Semaphore semaphore;

    // 运行标记，volatile保证多线程可见性，支持安全停止Worker
    private volatile boolean running = true;

    public Worker(String name, TaskQueue taskQueue, TaskScheduler scheduler, Semaphore semaphore) {
        super(name);
        this.taskQueue = taskQueue;
        this.scheduler = scheduler;
        this.semaphore = semaphore;
    }

    public void shutdown() {
        running = false;
        this.interrupt();
    }

    @Override
    public void run() {
        while (running) {
            try {
                // take()在队列为空时阻塞，避免空转消耗CPU
                Task task = taskQueue.take();

                // Semaphore限流：同时最多允许N个任务执行
                semaphore.acquire();

                // 更新状态为RUNNING
                scheduler.updateState(task.getTaskId(), TaskState.RUNNING);
                scheduler.printState(task.getTaskId(), TaskState.RUNNING);

                // 异步执行任务，避免Worker阻塞
                scheduler.executeWithRetryAsync(task)
                        .whenComplete((result, throwable) -> {
                            try {
                                if (throwable != null || (result != null && !result.isSuccess())) {
                                    scheduler.updateState(task.getTaskId(), TaskState.FAILED);
                                    scheduler.printState(task.getTaskId(), TaskState.FAILED);
                                } else {
                                    scheduler.updateState(task.getTaskId(), TaskState.SUCCESS);
                                    scheduler.printState(task.getTaskId(), TaskState.SUCCESS);
                                }
                            } finally {
                                // 通知任务完成，并释放限流许可
                                scheduler.countDownDone();
                                semaphore.release();
                            }
                        });

            } catch (InterruptedException ex) {
                if (!running) {
                    break;
                }
            }
        }
    }
}
