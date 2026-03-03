package scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 任务调度中心（核心）
 *
 * 说明：
 * 1. 参考ThreadPoolExecutor设计思想：核心Worker线程 + 任务队列。
 * 2. 任务状态用ConcurrentHashMap存储，支持高并发读写。
 * 3. 使用ReentrantLock保证关键状态更新的原子性（例如初始化CountDownLatch）。
 * 4. 使用Semaphore控制最大并发任务数，避免过载。
 * 5. 使用CountDownLatch实现waitAllTasksDone通知。
 * 6. 使用CompletableFuture实现异步执行、超时控制与回调。
 */
public class TaskScheduler {

    // 线程池核心线程数（Worker数量）
    // 说明：类似ThreadPoolExecutor的corePoolSize，决定常驻Worker数量
    private final int corePoolSize;

    // 最大线程数（用于异步执行线程池上限）
    // 说明：类似ThreadPoolExecutor的maximumPoolSize，用于限制异步执行线程池规模
    private final int maximumPoolSize;

    // 任务队列
    // 说明：BlockingQueue是线程安全的生产者-消费者队列，put/take自动阻塞
    private final TaskQueue taskQueue;

    // 任务ID生成器
    // 说明：AtomicInteger基于CAS，无需锁即可保证ID唯一性
    private final TaskIdGenerator idGenerator = new TaskIdGenerator();

    // 任务状态表：key=taskId, value=TaskState
    // 说明：ConcurrentHashMap支持高并发读写，避免HashMap + synchronized的锁竞争
    private final Map<Integer, TaskState> taskStates = new ConcurrentHashMap<>();

    // ReentrantLock确保关键状态更新线程安全
    // 说明：用于保护CountDownLatch初始化与读取的原子性，避免竞态条件
    private final ReentrantLock lock = new ReentrantLock();

    // 任务完成通知
    // 说明：CountDownLatch用于等待一批任务完成，不可重复使用
    private CountDownLatch doneLatch;

    // 限流控制，最多允许N个任务并发执行
    // 说明：Semaphore基于AQS，实现最大并发任务数限制，防止系统过载
    private final Semaphore semaphore;

    // Worker线程列表
    // 说明：Worker线程不断从队列take任务，形成固定工作线程池
    private final List<Worker> workers = new ArrayList<>();

    // 异步执行线程池（用于CompletableFuture）
    // 说明：CompletableFuture异步执行需要Executor支持，避免阻塞Worker线程
    private final ExecutorService asyncExecutor;

    public TaskScheduler(int corePoolSize, int maximumPoolSize, int queueCapacity, int maxConcurrentTasks) {
        this.corePoolSize = corePoolSize;
        this.maximumPoolSize = maximumPoolSize;
        this.taskQueue = new TaskQueue(queueCapacity);
        this.semaphore = new Semaphore(maxConcurrentTasks);
        this.asyncExecutor = Executors.newFixedThreadPool(maximumPoolSize);
    }

    /**
     * 预设本次批量任务数量
     * CountDownLatch不能增加计数，因此需要在提交前初始化
     */
    public void setExpectedTasks(int totalTasks) {
        lock.lock();
        try {
            this.doneLatch = new CountDownLatch(totalTasks);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 启动Worker线程
     */
    public void start() {
        for (int i = 0; i < corePoolSize; i++) {
            Worker worker = new Worker("worker-" + (i + 1), taskQueue, this, semaphore);
            workers.add(worker);
            worker.start();
        }
    }

    /**
     * 提交任务
     */
    public void submit(Task task) {
        int id = idGenerator.nextId();
        task.setTaskId(id);

        // 更新状态为PENDING
        updateState(id, TaskState.PENDING);
        printState(id, TaskState.PENDING);

        try {
            taskQueue.put(task);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            updateState(id, TaskState.FAILED);
            printState(id, TaskState.FAILED);
        }
    }

    /**
     * 等待所有任务完成
     */
    public void waitAllTasksDone() {
        CountDownLatch latch;
        lock.lock();
        try {
            latch = this.doneLatch;
        } finally {
            lock.unlock();
        }
        if (latch == null) {
            return;
        }
        try {
            latch.await();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 带超时等待
     */
    public boolean waitAllTasksDone(long timeoutMillis) {
        CountDownLatch latch;
        lock.lock();
        try {
            latch = this.doneLatch;
        } finally {
            lock.unlock();
        }
        if (latch == null) {
            return true;
        }
        try {
            return latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * 任务完成后调用countDown
     */
    public void countDownDone() {
        lock.lock();
        try {
            if (doneLatch != null) {
                doneLatch.countDown();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 更新任务状态
     */
    public void updateState(int taskId, TaskState state) {
        taskStates.put(taskId, state);
    }

    public TaskState getState(int taskId) {
        return taskStates.get(taskId);
    }

    /**
     * 简单打印状态（模拟生产环境日志）
     */
    public void printState(int taskId, TaskState state) {
        String threadName = Thread.currentThread().getName();
        System.out.println("Task " + taskId + " " + state + " thread=" + threadName);
    }

    /**
     * 打印重试信息
     */
    public void printRetry(int taskId, int attempt, int maxRetries) {
        String threadName = Thread.currentThread().getName();
        System.out.println("Task " + taskId + " RETRY " + attempt + "/" + maxRetries + " thread=" + threadName);
    }

    /**
     * 异步执行任务（带重试与超时控制）
     */
    public CompletableFuture<TaskResult> executeWithRetryAsync(Task task) {
        return executeWithRetryInternal(task, 1);
    }

    private CompletableFuture<TaskResult> executeWithRetryInternal(Task task, int attempt) {
        return executeOnceAsync(task).thenCompose(result -> {
            if (result.isSuccess() || attempt > task.getMaxRetries()) {
                return CompletableFuture.completedFuture(result);
            }
            printRetry(task.getTaskId(), attempt, task.getMaxRetries());
            return executeWithRetryInternal(task, attempt + 1);
        });
    }

    /**
     * 单次异步执行任务，支持超时
     */
    private CompletableFuture<TaskResult> executeOnceAsync(Task task) {
        long start = System.currentTimeMillis();
        CompletableFuture<Object> future = CompletableFuture.supplyAsync(() -> {
            try {
                return task.execute();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }, asyncExecutor);

        CompletableFuture<Object> timedFuture = future;
        if (task.getTimeoutMillis() > 0) {
            timedFuture = future.orTimeout(task.getTimeoutMillis(), TimeUnit.MILLISECONDS);
        }

        return timedFuture.handle((value, ex) -> {
            long end = System.currentTimeMillis();
            if (ex == null) {
                return TaskResult.success(task.getTaskId(), task.getName(), value, end - start);
            }
            Exception real;
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            if (cause instanceof TimeoutException) {
                // 超时则尝试中断执行线程，避免资源浪费
                future.cancel(true);
                real = (TimeoutException) cause;
            } else if (cause instanceof Exception) {
                real = (Exception) cause;
            } else {
                real = new Exception(cause);
            }
            return TaskResult.failed(task.getTaskId(), task.getName(), real, end - start);
        });
    }

    /**
     * 关闭Worker与异步线程池
     */
    public void shutdown() {
        for (Worker worker : workers) {
            worker.shutdown();
        }
        asyncExecutor.shutdown();
    }
}
