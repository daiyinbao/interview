package scheduler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 任务队列
 *
 * 说明：
 * 1. 使用BlockingQueue保证线程安全的入队/出队。
 * 2. put/take在队列满/空时自动阻塞，天然实现生产者-消费者模型。
 */
public class TaskQueue {

    // 采用LinkedBlockingQueue：链表结构，put/take使用内部锁 + 条件变量保障线程安全
    // 说明：BlockingQueue天然支持生产者-消费者模型，避免自行处理同步与阻塞
    private final BlockingQueue<Task> queue;

    public TaskQueue(int capacity) {
        this.queue = new LinkedBlockingQueue<>(capacity);
    }

    public void put(Task task) throws InterruptedException {
        queue.put(task);
    }

    public Task take() throws InterruptedException {
        return queue.take();
    }

    public int size() {
        return queue.size();
    }
}
