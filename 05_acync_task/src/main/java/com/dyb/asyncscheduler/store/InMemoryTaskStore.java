/**
 * @author dai_yin_bao
 * @date 2026/3/3 23:47
 * @file InMemoryTaskStore.java
 */
package com.dyb.asyncscheduler.store;

import com.dyb.asyncscheduler.task.EnqueueState;
import com.dyb.asyncscheduler.task.Task;
import com.dyb.asyncscheduler.task.TaskState;
import com.dyb.asyncscheduler.util.DebugLog;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务状态机的内存版持久层实现
 * 安全地推进任务状态
 * 安全地控制执行权（lease）
 * 保证多线程下状态一致
 */
public  final class InMemoryTaskStore implements TaskStore{
    //用于存放任务
    private final ConcurrentHashMap<String,Task> tasks = new ConcurrentHashMap<>();

    @Override
    public void insert(Task task) {
        Objects.requireNonNull(task,"task");
        DebugLog.log("Store insert taskId=%s state(before)=%s", task.getTaskId(), task.state);
        //添加任务，如果该id之前存在则返回并覆盖旧任务
        Task prev = tasks.put(task.getTaskId(), task);
        if(prev !=null){
            //重复任务id
            throw new IllegalStateException("duplicate taskId");
        }
        // 该项目当前版本没有单独的 Scheduler 去推进 NEW -> READY。
        // 如果仍停留在 NEW，Worker.tryLease() 只接受 READY/RETRY，会导致任务永远无法执行。
        task.state=TaskState.READY;
        //默认设置立即可执行
        task.nextRunAtEpochMs=0L;
        DebugLog.log("Store insert done taskId=%s state(after)=%s nextRunAt=%d", task.getTaskId(), task.state, task.nextRunAtEpochMs);

    }

    @Override
    public Optional<Task> get(String taskId) {
        return Optional.ofNullable(tasks.get(taskId));
    }

    /**
     * 检测任务的期权是否合法，若不合法，则可以重新调度
     * @param taskId
     * @param owner
     * @param nowMs
     * @param ttlMs
     * @return
     */
    @Override
    public boolean tryLease(String taskId, String owner, long nowMs, long ttlMs) {
        Task t = tasks.get(taskId);
        DebugLog.log("Store tryLease enter taskId=%s owner=%s now=%d ttl=%d", taskId, owner, nowMs, ttlMs);
        if(t == null){
            DebugLog.log("Store tryLease missing taskId=%s", taskId);
            return false;
        }

        synchronized (t){
            DebugLog.log("Store tryLease check taskId=%s state=%s nextRunAt=%d leaseOwner=%s leaseUntil=%d",
                    taskId, t.state, t.nextRunAtEpochMs, t.leaseOwner, t.leaseUntilEpochMs);
            //如果任务不能被调度
            if(!(t.state == TaskState.READY || t.state == TaskState.RETRY)) {
                DebugLog.log("Store tryLease reject taskId=%s reason=state state=%s", taskId, t.state);
                return false;
            }
            //未到执行时间
            if(t.nextRunAtEpochMs > nowMs) {
                DebugLog.log("Store tryLease reject taskId=%s reason=not_due nextRunAt=%d now=%d", taskId, t.nextRunAtEpochMs, nowMs);
                return false;
            }
            //期权仍合法
            if(t.leaseOwner != null && t.leaseUntilEpochMs >nowMs) {
                DebugLog.log("Store tryLease reject taskId=%s reason=leased leaseOwner=%s leaseUntil=%d now=%d",
                        taskId, t.leaseOwner, t.leaseUntilEpochMs, nowMs);
                return false;
            }
            t.attempt +=1;
            //已派发
            t.state= TaskState.DISPATCHED;
            t.leaseOwner = owner;
            t.leaseUntilEpochMs = nowMs + ttlMs;
            DebugLog.log("Store tryLease success taskId=%s newState=%s leaseOwner=%s leaseUntil=%d attempt=%d",
                    taskId, t.state, t.leaseOwner, t.leaseUntilEpochMs, t.attempt);
            return true;
        }

    }

    /**
     * 修改特定任务的状态是RUNNING
     * @param taskId
     * @param expectedState “期望”当前任务正处于的状态
     * @return
     */
    @Override
    public boolean markRunning(String taskId, TaskState expectedState) {
        Task t = tasks.get(taskId);
        if(t == null) {
            DebugLog.log("Store markRunning missing taskId=%s expected=%s", taskId, expectedState);
            return false;
        }
        synchronized (t){
            //当状态是我认为的那个状态时，才允许修改
            if (t.state != expectedState) {
                DebugLog.log("Store markRunning reject taskId=%s expected=%s actual=%s", taskId, expectedState, t.state);
                return false;
            }
            t.state = TaskState.RUNNING;
            DebugLog.log("Store markRunning success taskId=%s newState=%s", taskId, t.state);
            return true;
        }
    }

    /**
     * 处理成功状态
     * @param taskId
     */
    @Override
    public void completeSuccess(String taskId) {
        Task t = tasks.get(taskId);
        if(t ==null) {
            DebugLog.log("Store completeSuccess missing taskId=%s", taskId);
            return;
        }
        synchronized(t){
            t.state = TaskState.SUCCESS;
            t.lastError = null;
            t.nextRunAtEpochMs = 0L;
            //修改入队状态
            t.enqueueState= EnqueueState.NONE;
            t.enqueueUntilEpochMs = 0L;
            DebugLog.log("Store completeSuccess taskId=%s newState=%s", taskId, t.state);
        }

    }

    /**
     * 处理失败的任务
     * @param taskId
     * @param error
     * @param retryable
     * @param nextRunAtEpochMs
     */
    @Override
    public void completeFailure(String taskId, String error, boolean retryable, long nextRunAtEpochMs) {
        Task t = tasks.get(taskId);
        if (t == null) {
            DebugLog.log("Store completeFailure missing taskId=%s", taskId);
            return;
        }
        synchronized(t){
            t.lastError = error;
            t.state = TaskState.FAILED;
            boolean canRetry = retryable && (t.attempt<t.maxAttempts);
            //如果任务失败，判断该任务是否可以重试
            if(canRetry){
                t.state=TaskState.RETRY;
                t.nextRunAtEpochMs = nextRunAtEpochMs;
            }else {
                t.state =TaskState.DEAD;
                t.nextRunAtEpochMs = 0L;
            }
            //都是未入队状态
            t.enqueueState = EnqueueState.NONE;
            t.enqueueUntilEpochMs = 0L;
            DebugLog.log("Store completeFailure taskId=%s newState=%s attempt=%d maxAttempts=%d nextRunAt=%d error=%s",
                    taskId, t.state, t.attempt, t.maxAttempts, t.nextRunAtEpochMs, t.lastError);
        }
    }

    /**
     * 释放期权
     * @param taskId
     * @param owner
     */
    @Override
    public void releaseLease(String taskId, String owner) {
        Task t = tasks.get(taskId);
        if (t == null) {
            DebugLog.log("Store releaseLease missing taskId=%s owner=%s", taskId, owner);
            return;
        }
        synchronized(t){
            DebugLog.log("Store releaseLease taskId=%s owner(param)=%s leaseOwner(before)=%s leaseUntil(before)=%d",
                    taskId, owner, t.leaseOwner, t.leaseUntilEpochMs);
            t.leaseOwner = null;
            t.leaseUntilEpochMs=0L;
            DebugLog.log("Store releaseLease done taskId=%s leaseOwner(after)=%s leaseUntil(after)=%d",
                    taskId, t.leaseOwner, t.leaseUntilEpochMs);
        }
    }
}
