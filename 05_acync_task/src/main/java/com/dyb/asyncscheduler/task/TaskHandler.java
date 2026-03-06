/**
 * @author dai_yin_bao
 * @date 2026/3/4 14:45
 * @file TaskHandler.java
 */
package com.dyb.asyncscheduler.task;

public interface TaskHandler {
    TaskResult execute(TaskContext ctx) throws Exception;
}
