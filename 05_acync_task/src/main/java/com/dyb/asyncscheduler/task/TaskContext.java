/**
 * @author dai_yin_bao
 * @date 2026/3/4 14:46
 * @file TaskContext.java
 */
package com.dyb.asyncscheduler.task;

/**
 * 传递任务需要的内容
 */
public record TaskContext(
        String taskId,
        String type,
        String payloadJson,
        int attempt,
        String shard
) {

}
