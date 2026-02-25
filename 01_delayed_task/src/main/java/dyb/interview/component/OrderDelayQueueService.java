package dyb.interview.component;

import org.redisson.api.RBlockingQueue;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class OrderDelayQueueService {

    private static final String QUEUE_NAME = "orderDelayQueue";

    @Autowired
    private RedissonClient redissonClient;

    public void addDelayOrder(Long orderId, long delayMinutes) {

        RBlockingQueue<Long> blockingQueue = redissonClient.getBlockingQueue(QUEUE_NAME);

        RDelayedQueue<Long> delayedQueue = redissonClient.getDelayedQueue(blockingQueue);

        delayedQueue.offer(orderId, delayMinutes, TimeUnit.SECONDS);
    }
}