/**
 * @author dai_yin_bao
 * @date 2026/3/4 10:33
 * @file TaskRunner.java
 */
package com.dyb.asyncscheduler.worker;

import com.dyb.asyncscheduler.store.TaskStore;
import com.dyb.asyncscheduler.task.TaskState;

/**
 * 任务执行状态机的驱动器
 * Worker 线程真正执行任务状态流转的执行器。Worker 线程池会运行
 */
public final class TaskRunner implements Runnable{

    private final String taskId;
    private final TaskStore store;
    private final String owner;

    public TaskRunner(String taskId, TaskStore store, String owner) {
        this.taskId = taskId;
        this.store = store;
        this.owner = owner;
    }

    @Override
    public void run() {
        if(!store.markRunning(taskId, TaskState.DISPATCHED)){
            store.releaseLease(taskId,owner);
            return;
        }

        try {
            //真正执行任务
            store.completeSuccess(taskId);
        } catch (Exception ex) {
            //失败重试机制
            long nextRunAt = System.currentTimeMillis() + 200L;
            store.completeFailure(taskId, ex.getMessage(), true, nextRunAt);
        } finally {
            store.releaseLease(taskId, owner);
        }

    }
}
