package scheduler;

/**
 * 任务状态枚举
 *
 * 说明：
 * PENDING  : 已提交到系统，但尚未开始执行
 * RUNNING  : 正在执行中
 * SUCCESS  : 执行成功
 * FAILED   : 执行失败（发生异常或超时）
 */
public enum TaskState {
    PENDING,
    RUNNING,
    SUCCESS,
    FAILED
}
