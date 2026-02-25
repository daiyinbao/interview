package dyb.interview.service;

import dyb.interview.component.OrderDelayQueueService;
import dyb.interview.entity.Order;
import dyb.interview.mapper.OrderMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class OrderService {

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private OrderDelayQueueService delayQueueService;

    // 下单
    @Transactional
    public Long createOrder(Long userId) {

        Order order = new Order();
        order.setOrderNo(UUID.randomUUID().toString());
        order.setUserId(userId);
        order.setStatus(0);

        orderMapper.insert(order);

        // 加入延迟队列（30分钟）
        delayQueueService.addDelayOrder(order.getId(), 30);

        return order.getId();
    }

    // 支付
    @Transactional
    public void payOrder(Long orderId) {

        int rows = orderMapper.payIfUnpaid(orderId);

        if (rows == 0) {
            throw new RuntimeException("订单已取消或已支付");
        }
    }

    // 延迟取消
    @Transactional
    public void handleExpireOrder(Long orderId) {

        int rows = orderMapper.cancelIfUnpaid(orderId);

        if (rows > 0) {
            System.out.println("订单超时取消: " + orderId);
        }
    }
}