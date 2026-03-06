package com.dyb.asyncscheduler.task;

import lombok.Data;

/**
 * 执行任务实例
 */
@Data
public final class Task {
    //任务的唯一标识
    private final String taskId;
    //任务的类型-->根据type选择不同的handler来执行任务
    private final String type;
    //任务的数据内容
    private final String payloadJson;

    //幂等键-->防止重复提交任务
    public final String idempotencyKey;
    //分片键-->worker分片执行
    public final String shard;

    public final int schemaVersion;
    public final long timeoutMs;

    //任务的状态
    public volatile TaskState state;
    //任务的重试次数
    public volatile int attempt;
    //任务的最大重试次数
    public volatile int maxAttempts;

    //调度时间控制-->延迟时间，超过该时间 则不会调度
    //任务在这个时间点之前，不能被调度执行
    public volatile long nextRunAtEpochMs;

    //防止scheduler把同一个任务重复放进readyQueue
    public volatile EnqueueState enqueueState;
    //务在这个时间之前已经入队过
    public volatile long enqueueUntilEpochMs;

    //记录当前执行任务的worker
    public volatile String leaseOwner;
    //执行权什么时候过期
    public volatile long leaseUntilEpochMs;

    //上次执行失败原因
    public volatile String lastError;

    //任务重试策略
    public final RetryPolicy retryPolicy;


    public Task(
            String taskId,
            String type,
            String payloadJson,
            String idempotencyKey,
            String shard,
            int schemaVersion,
            long timeoutMs,
            RetryPolicy retryPolicy
    ) {
        this.taskId = taskId;
        this.type = type;
        this.payloadJson = payloadJson;
        this.idempotencyKey = idempotencyKey;
        this.shard = shard;

        this.schemaVersion = schemaVersion;
        this.timeoutMs = timeoutMs;
        this.retryPolicy = retryPolicy;

        this.state = TaskState.NEW;
        this.attempt = 0;
        this.maxAttempts = 3;
        this.nextRunAtEpochMs = 0L;

        this.enqueueState = EnqueueState.NONE;
        this.enqueueUntilEpochMs = 0L;

        this.leaseOwner = null;
        this.leaseUntilEpochMs = 0L;

        this.lastError = null;
    }
}
