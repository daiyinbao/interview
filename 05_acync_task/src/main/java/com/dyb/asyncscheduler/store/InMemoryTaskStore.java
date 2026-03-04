/**
 * @author dai_yin_bao
 * @date 2026/3/3 23:47
 * @file InMemoryTaskStore.java
 */
package com.dyb.asyncscheduler.store;

import com.dyb.asyncscheduler.task.Task;
import com.dyb.asyncscheduler.task.TaskState;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public  final class InMemoryTaskStore implements TaskStore{
    //用于存放任务
    private final ConcurrentHashMap<String,Task> tasks = new ConcurrentHashMap<>();

    @Override
    public void insert(Task task) {
        Objects.requireNonNull(task,"task");
        //添加任务，如果该id之前存在则返回并覆盖旧任务
        Task prev = tasks.put(task.getTaskId(), task);
        if(prev !=null){
            //重复任务id
            throw new IllegalStateException("duplicate taskId");
        }
        task.state=TaskState.NEW;
        //默认设置立即可执行
        task.nextRunAtEpochMs=0L;

    }

    @Override
    public Optional<Task> get(String taskId) {
        return Optional.ofNullable(tasks.get(taskId));
    }

    @Override
    public boolean tryLease(String taskId, String owner, long nowMs, long ttlMs) {

        return false;
    }

    @Override
    public boolean markRunning(String taskId, TaskState expectedState) {
        return false;
    }

    @Override
    public void completeSuccess(String taskId) {

    }

    @Override
    public void completeFailure(String taskId, String error, boolean retryable, long nextRunAtEpochMs) {

    }

    @Override
    public void releaseLease(String taskId, String owner) {

    }
}
