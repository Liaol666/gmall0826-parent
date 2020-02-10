package com.atguigu.gmall0826.publisher.service.impl;

import com.atguigu.gmall0826.publisher.mapper.DauMapper;
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
            hourCountMap.put ( map.get("lh"),map.get("ct"));
        }
        return hourCountMap;
    }
}
