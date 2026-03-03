package scheduler;

import java.util.concurrent.Callable;

/**
 * 任务抽象
 *
 * 说明：
 * 1. 使用Callable而不是Runnable，方便返回执行结果或抛出异常。
 * 2. 任务ID由调度器生成并注入，避免业务侧自行生成导致冲突。
 */
public class Task implements Callable<TaskResult> {

    // 任务ID（由TaskScheduler统一生成）
    private int taskId;

    // 业务名称，便于日志和排查
    private final String name;

    // 任务真正的业务逻辑
    // 说明：外部传入的Callable，支持返回结果或抛出异常
    private final Callable<Object> action;

    // 最大重试次数（0表示不重试）
    private final int maxRetries;

    // 单次执行超时时间（毫秒，0表示不限制）
    private final long timeoutMillis;

    public Task(String name, Callable<Object> action) {
        this(name, action, 0, 0);
    }

    public Task(String name, Callable<Object> action, int maxRetries, long timeoutMillis) {
        this.name = name;
        this.action = action;
        this.maxRetries = maxRetries;
        this.timeoutMillis = timeoutMillis;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public String getName() {
        return name;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    /**
     * 执行原始业务逻辑（供调度器做重试与超时控制）
     */
    public Object execute() throws Exception {
        return action.call();
    }

    /**
     * 任务执行入口（用于同步调用）
     * 返回TaskResult，包含执行耗时和异常信息
     */
    @Override
    public TaskResult call() throws Exception {
        long start = System.currentTimeMillis();
        try {
            Object result = action.call();
            long end = System.currentTimeMillis();
            return TaskResult.success(taskId, name, result, end - start);
        } catch (Exception ex) {
            long end = System.currentTimeMillis();
            return TaskResult.failed(taskId, name, ex, end - start);
        }
    }
}
