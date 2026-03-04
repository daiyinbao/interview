/**
 * @author dai_yin_bao
 * @date 2026/3/4 14:47
 * @file TaskResult.java
 */
package com.dyb.asyncscheduler.task;

/**
 * 任务执行结果的返回
 */
public class TaskResult {
    public final boolean success;
    public final boolean retryable;

    public final  String errorCode;
    public final String message;

    private TaskResult(boolean success, boolean retryable, String errorCode, String message) {
        this.success = success;
        this.retryable = retryable;
        this.errorCode = errorCode;
        this.message = message;
    }
    public static TaskResult ok() {
        return new TaskResult(true, false, null, null);
    }

    public static TaskResult retryableFailure(String errorCode, String message) {
        return new TaskResult(false, true, errorCode, message);
    }

    public static TaskResult nonRetryableFailure(String errorCode, String message) {
        return new TaskResult(false, false, errorCode, message);
    }
}
