/**
 * @author dai_yin_bao
 * @date 2026/3/4 10:19
 * @file InMemoryTaskQueue.java
 */
package com.dyb.asyncscheduler.queue;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于内存的就绪队列实现
 */
public  final class InMemoryTaskQueue implements TaskQueue{
    //有界就绪队列-->底层是环形队列
    //立即可执行任务
    private final BlockingQueue<String> ready;
    //延迟队列任务
    private final DelayQueue<DelayedTask> delay  = new DelayQueue<>();
    //是否接受新任务
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    //transferThread 是否继续运行
    private final AtomicBoolean running = new AtomicBoolean(true);
    //提交任务被拒绝的次数
    private AtomicLong offerRejectCount = new AtomicLong(0L);
    //延迟任务转移失败次数
    private AtomicLong requeueCont = new AtomicLong(0L);
    //核心线程,把 delay queue 的任务转移到 ready queue
    private final Thread transferThread;



    public InMemoryTaskQueue(int readyCapacity) {
        if (readyCapacity <= 0) throw new IllegalArgumentException("readyCapacity must be > 0");
        this.ready = new ArrayBlockingQueue<>(readyCapacity);

        this.transferThread = new Thread(this::transferLoop, "queue-transfer-1");
        //设置守护线程-->JVM退出时自动退出
        this.transferThread.setDaemon(true);
        this.transferThread.start();
    }



    @Override
    public boolean offer(String taskId, long nextRunAtEpochMs) {
        Objects.requireNonNull(taskId,"taskId");
        if(!accepting.get()) return false;
        long now = System.currentTimeMillis();
        //如果该任务达到执行时间
        if(nextRunAtEpochMs<= now){
            //取出任务
            boolean ok = ready.offer(taskId);
            //若队列已满，提交任务被拒绝
            if(!ok) offerRejectCount.incrementAndGet();
            return ok;
        }
        //加入延时队列
        delay.offer(new DelayedTask(taskId,nextRunAtEpochMs));
        return true;
    }

    //带超时 offer,ready 满时等待 timeout
    @Override
    public boolean offer(String taskId, long nextRunAtEpochMs, long timeoutMs) throws InterruptedException {
        Objects.requireNonNull(taskId,"taskId");
        if(!accepting.get()) return false;
        long now = System.currentTimeMillis();
        if(nextRunAtEpochMs<=now){
            boolean ok = ready.offer(taskId,timeoutMs,TimeUnit.MILLISECONDS);
            if(!ok) offerRejectCount.incrementAndGet();
            return ok;
        }
        delay.offer(new DelayedTask( taskId,nextRunAtEpochMs));

        return true;
    }

    /**
     * 从队列中取任务
     * @return
     * @throws InterruptedException
     */
    @Override
    public String take() throws InterruptedException {
        return ready.take();
    }

    @Override
    public int readySize() {
        return ready.size();
    }

    @Override
    public int readyCapacity() {
        return ready.size() + ready.remainingCapacity();
    }

    @Override
    public int delaySize() {
        return delay.size();
    }

    @Override
    public void shutdown() {
        accepting.set(false);
        running.set(false);
        transferThread.interrupt();
    }

    public long offerRejectCount() {
        return offerRejectCount.get();
    }

    public long requeueCount() {
        return requeueCont.get();
    }


    private void transferLoop(){
        while (running.get() && !Thread.currentThread().isInterrupted()){
            try {
                //阻塞直到到期
                DelayedTask due = delay.take();
                //ready满时，使用短超时；失败后重试
                boolean ok = ready.offer(due.taskId, 50L, TimeUnit.MILLISECONDS);
                if(!ok){
                    requeueCont.incrementAndGet();
                    delay.offer(new DelayedTask(due.taskId,System.currentTimeMillis()));

                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }catch (Exception ex){
                try {
                    Thread.sleep(50L);
                }catch (InterruptedException ie){
                    Thread.currentThread().interrupt();
                    break;
                }
            }

        }
    }

}
