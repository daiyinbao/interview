package scheduler;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 任务ID生成器
 *
 * 说明：
 * 1. 使用AtomicInteger进行ID自增，保证多线程下安全。
 * 2. AtomicInteger内部基于CAS实现，无需加锁，性能高。
 */
public class TaskIdGenerator {

    // AtomicInteger基于CAS实现线程安全，避免多线程下ID重复
    private final AtomicInteger counter = new AtomicInteger(0);

    public int nextId() {
        return counter.incrementAndGet();
    }
}
