package scheduler;

/**
 * 任务执行结果
 *
 * 说明：
 * 1. 统一封装执行结果、异常、耗时，便于调度器记录与回调。
 * 2. 成功与失败通过静态工厂方法创建，语义清晰。
 */
public class TaskResult {

    // 任务ID，便于状态映射与日志定位
    private final int taskId;

    // 任务名称，提升可读性
    private final String taskName;

    // 是否成功
    private final boolean success;

    // 成功结果（失败时为null）
    private final Object result;

    // 失败异常（成功时为null）
    private final Exception exception;

    // 执行耗时（毫秒）
    private final long costMillis;

    private TaskResult(int taskId, String taskName, boolean success, Object result, Exception exception, long costMillis) {
        this.taskId = taskId;
        this.taskName = taskName;
        this.success = success;
        this.result = result;
        this.exception = exception;
        this.costMillis = costMillis;
    }

    public static TaskResult success(int taskId, String taskName, Object result, long costMillis) {
        return new TaskResult(taskId, taskName, true, result, null, costMillis);
    }

    public static TaskResult failed(int taskId, String taskName, Exception exception, long costMillis) {
        return new TaskResult(taskId, taskName, false, null, exception, costMillis);
    }

    public int getTaskId() {
        return taskId;
    }

    public String getTaskName() {
        return taskName;
    }

    public boolean isSuccess() {
        return success;
    }

    public Object getResult() {
        return result;
    }

    public Exception getException() {
        return exception;
    }

    public long getCostMillis() {
        return costMillis;
    }
}
