package com.dyb.asyncscheduler.scheduler;

/**
 * 调度器自身状态
 */
public enum SchedulerState {
    INIT,
    RUNNING,
    PAUSED,
    DEGRADED,//降级运行
    DRAINING,//优雅关闭
    STOPPED
}
