package dyb.interview.controller;

import dyb.interview.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/order")
public class OrderController {

    @Autowired
    private OrderService orderService;

    @PostMapping("/create")
    public Long create(@RequestParam Long userId) {
        return orderService.createOrder(userId);
    }

    @PostMapping("/pay")
    public String pay(@RequestParam Long orderId) {
        orderService.payOrder(orderId);
        return "支付成功";
    }
}
