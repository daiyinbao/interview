/**
 * @author dai_yin_bao
 * @date 2026/3/4 15:18
 * @file HandlerRegistry.java
 */
package com.dyb.asyncscheduler.task;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务处理器注册中心
 * 根据任务类型，找到对应的处理器来执行任务。
 */
public class HandlerRegistry {
    //映射: type--> TaskHandler
    private final ConcurrentHashMap<String, TaskHandler> handlers = new ConcurrentHashMap<>();

    public void register(String type,TaskHandler handler){
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(handler, "handler");
        //注册-->并检验是否存在
        TaskHandler prev = handlers.putIfAbsent(type, handler);
        //已存在
        if (prev != null) {
            throw new IllegalStateException("duplicate handler for type: " + type);
        }
    }

    public TaskHandler get(String type) {
        TaskHandler h = handlers.get(type);
        if (h == null) {
            throw new IllegalStateException("no handler registered for type: " + type);
        }
        return h;
    }

}
