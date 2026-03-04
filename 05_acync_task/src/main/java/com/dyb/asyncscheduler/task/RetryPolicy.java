/**
 * @author dai_yin_bao
 * @date 2026/3/4 15:24
 * @file RetryPolicy.java
 */
package com.dyb.asyncscheduler.task;

/**
 * 任务重试策略（Retry Strategy）实现类
 * 当任务失败后，延迟多久在重新执行
 */
public final class RetryPolicy {
    //初始重试延迟
    public final long baseDelayMs;
    //最大延迟上限
    public final long maxDelayMs;

    private RetryPolicy(long baseDelayMs , long maxDelayMs) {
        this.baseDelayMs = baseDelayMs;
        this.maxDelayMs = maxDelayMs;
    }

    //两种延迟模式
    //固定延迟
    public static RetryPolicy fixed(long delayMs){
        return new RetryPolicy(delayMs,delayMs);
    }
    //指数退避重试-->重试时间逐渐增加直至maxDelayMs
    public static RetryPolicy expBackoff(long baseDelayMs, long maxDelayMs) {
        return new RetryPolicy(baseDelayMs, maxDelayMs);
    }

    /**
     * 根据attempt决定下次重试的间隔时间
     * @param attempt
     * @return
     */
    public long nextDelayMs(int attempt){
        //尝试次数从1开始
        if(attempt<=1) return baseDelayMs;
        long d =  baseDelayMs << Math.min(10,attempt -1);
        if(d<0) d = maxDelayMs;
        return Math.min(d,maxDelayMs);
    }



}
