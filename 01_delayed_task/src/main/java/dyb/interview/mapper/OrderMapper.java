package dyb.interview.mapper;

import dyb.interview.entity.Order;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface OrderMapper {

    void insert(Order order);

    Order selectById(Long id);

    int cancelIfUnpaid(Long id);

    int payIfUnpaid(Long id);
}