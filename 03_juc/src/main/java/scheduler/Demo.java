package scheduler;

import java.util.Random;

/**
 * Demo模拟真实生产环境
 *
 * 需求：
 * 1. 提交100个任务
 * 2. 5个Worker并发执行
 * 3. 打印任务执行线程、状态变化和执行时间
 */
public class Demo {

    public static void main(String[] args) {
        int totalTasks = 100;

        // corePoolSize=5, maximumPoolSize=10, queueCapacity=1000, maxConcurrentTasks=5
        TaskScheduler scheduler = new TaskScheduler(5, 10, 1000, 5);

        // 预设任务数量，便于CountDownLatch完成通知
        scheduler.setExpectedTasks(totalTasks);
        scheduler.start();

        Random random = new Random();

        for (int i = 0; i < totalTasks; i++) {
            int taskIndex = i + 1;
            Task task = new Task("task-" + taskIndex, () -> {
                long start = System.currentTimeMillis();
                // 模拟业务执行耗时
                Thread.sleep(100 + random.nextInt(200));
                long end = System.currentTimeMillis();
                return "cost=" + (end - start) + "ms";
            }, 1, 500); // 示例：最多重试1次，单次超时500ms
            scheduler.submit(task);
        }

        // 等待全部任务完成
        scheduler.waitAllTasksDone();
        scheduler.shutdown();
    }
}
