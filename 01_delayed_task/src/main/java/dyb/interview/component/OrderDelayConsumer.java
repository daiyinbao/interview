package dyb.interview.component;

import dyb.interview.service.OrderService;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class OrderDelayConsumer implements InitializingBean, DisposableBean {

    private static final String QUEUE_NAME = "orderDelayQueue";

    @Autowired
    private RedissonClient redissonClient;

    @Autowired
    private OrderService orderService;

    private ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();

    private volatile boolean running = true;

    @Override
    public void afterPropertiesSet() {
        consumerExecutor.submit(() -> {
            RBlockingQueue<Long> blockingQueue = redissonClient.getBlockingQueue(QUEUE_NAME);
            while (running) {
                try {
                    Long orderId = blockingQueue.take();
                    System.out.println("收到过期订单: " + orderId);
                    if (orderId != null) {
                        orderService.handleExpireOrder(orderId);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }
    @Override
    public void destroy() {
        running = false;
        consumerExecutor.shutdown();
    }
}
