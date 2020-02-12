package com.atguigu.gmall0826.publisher.service.impl;

import com.atguigu.gmall0826.publisher.mapper.DauMapper;
import com.atguigu.gmall0826.publisher.mapper.OrderMapper;
import com.atguigu.gmall0826.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
   DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;


    @Override
    public Long getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHourCount(String date) {  //调整变换结构
        //   maplist ===> [{"loghour":"12","ct",500},{"loghour":"11","ct":400}......]
        List<Map> mapList = dauMapper.selectDauHourCount(date);
        // hourCountMap ==> {"12":500,"11":400,......}
        Map hourCountMap=new HashMap();
        for (Map map : mapList) {
            hourCountMap.put ( map.get("LOGHOUR"),map.get("CT"));
        }
        return hourCountMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmount(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        List<Map> mapList = orderMapper.selectOrderAmountHour(date);
        Map hourAmountMap=new HashMap();
        for (Map map : mapList) {
            hourAmountMap.put ( map.get("CREATE_HOUR"),map.get("ORDER_AMOUNT"));
        }
        return hourAmountMap;

    }
}
