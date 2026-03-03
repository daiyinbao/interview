package com.dyb.asyncscheduler.task;

/**
 * 任务的状态
 */
public enum TaskState {
    NEW,
    READY,
    DISPATCHED,//已派发
    RUNNING,
    SUCCESS,
    FAILED,
    RETRY,//重试
    DEAD,
    CANCELED,
    PAUSED

}
