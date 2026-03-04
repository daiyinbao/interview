/**
 * @author dai_yin_bao
 * @date 2026/3/3 23:13
 * @file TaskStore.java
 */
package com.dyb.asyncscheduler.store;

import com.dyb.asyncscheduler.task.Task;
import com.dyb.asyncscheduler.task.TaskState;

import java.util.Optional;

/**
 * 在“多线程 + 多进程 + 崩溃恢复”的环境下安全地推进任务状态机
 */
public interface TaskStore {
    //创建任务
    void insert(Task task);

    //查询任务
    Optional<Task> get(String taskId);

    //保证同一时间只有一个 worker 能执行任务,崩溃恢复
    boolean tryLease(String taskId, String owner,long nowMs,long ttlMs);

    //推进状态
    boolean markRunning(String taskId, TaskState expectedState);

    void completeSuccess(String taskId);

    void completeFailure(String taskId, String error, boolean retryable,long nextRunAtEpochMs);

    void releaseLease(String taskId, String owner);

}
