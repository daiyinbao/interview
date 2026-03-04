/**
 * @author dai_yin_bao
 * @date 2026/3/4 10:33
 * @file TaskRunner.java
 */
package com.dyb.asyncscheduler.worker;

import com.dyb.asyncscheduler.store.TaskStore;
import com.dyb.asyncscheduler.task.TaskState;
import com.dyb.asyncscheduler.util.DebugLog;

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
        DebugLog.log("TaskRunner start taskId=%s owner=%s", taskId, owner);
        //标记任务
        boolean runningMarked = store.markRunning(taskId, TaskState.DISPATCHED);
        DebugLog.log("TaskRunner markRunning taskId=%s ok=%s expected=%s", taskId, runningMarked, TaskState.DISPATCHED);
        if(!runningMarked){
            store.releaseLease(taskId,owner);
            DebugLog.log("TaskRunner markRunning failed; released lease taskId=%s owner=%s", taskId, owner);
            return;
        }

        try {
            //真正执行任务
            store.completeSuccess(taskId);
            DebugLog.log("TaskRunner completeSuccess taskId=%s", taskId);
        } catch (Exception ex) {
            //失败重试机制
            long nextRunAt = System.currentTimeMillis() + 200L;
            store.completeFailure(taskId, ex.getMessage(), true, nextRunAt);
            DebugLog.log("TaskRunner completeFailure taskId=%s error=%s nextRunAt=%d", taskId, ex.getMessage(), nextRunAt);
        } finally {
            //释放期权
            store.releaseLease(taskId, owner);
            DebugLog.log("TaskRunner releaseLease taskId=%s owner=%s", taskId, owner);
        }

    }
}
