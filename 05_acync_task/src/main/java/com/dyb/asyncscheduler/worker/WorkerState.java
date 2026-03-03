package com.dyb.asyncscheduler.worker;

/**
 * 调度器管理的执行单元的状态
 */
public enum WorkerState {
    IDLE,        // 空闲
    RESERVED,    // 已预占
    SUBMITTED,   // 已提交执行
    BACKOFF,     // 退避中
    DRAINING,    // 排空中
    STOPPED,     // 已停止
    UNHEALTHY    // 不健康
}
