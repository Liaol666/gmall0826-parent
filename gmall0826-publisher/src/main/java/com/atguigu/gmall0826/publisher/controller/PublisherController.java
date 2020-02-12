package com.atguigu.gmall0826.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0826.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {
    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String  getRealtimeTotal(@RequestParam("date") String date ){
        List<Map>  totalList=new ArrayList<>();
        Map dauMap=new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        Long dauTotal = publisherService.getDauTotal(date);
        dauMap.put("value",dauTotal);
        totalList.add(dauMap);

        Map newMidMap=new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);
        totalList.add(newMidMap);


        Map orderAmountMap=new HashMap();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        Double orderAmount = publisherService.getOrderAmount(date);
        orderAmountMap.put("value",orderAmount);
        totalList.add(orderAmountMap);

         return JSON.toJSONString(totalList);
    }

    @GetMapping("realtime-hour")
    public String   getRealtimeHour(@RequestParam("id")String id , @RequestParam("date")String date){
        // 利用实现的业务方法， 按照接口的返回值要求组装数据
        if("dau".equals(id)){
            Map dauHourCountTDMap = publisherService.getDauHourCount(date);
            String yd = getYesterday(date);
            Map dauHourCountYDMap = publisherService.getDauHourCount(yd);

            Map<String,Map> resultMap=new HashMap<>();
            resultMap.put("yesterday",dauHourCountYDMap);
            resultMap.put("today",dauHourCountTDMap);
            return   JSON.toJSONString(resultMap);
        }else if ("order_amount".equals(id)){  //新增交易额的分时统计
            Map orderAmontHourTDMap = publisherService.getOrderAmountHour(date);
            String yd = getYesterday(date);
            Map orderAmontHourYDMap = publisherService.getOrderAmountHour(yd);

            Map<String,Map> resultMap=new HashMap<>();
            resultMap.put("yesterday",orderAmontHourYDMap);
            resultMap.put("today",orderAmontHourTDMap);
            return   JSON.toJSONString(resultMap);
        }

        return  null;
    }

    private  String   getYesterday(String td){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        try {
            Date tdDate = simpleDateFormat.parse(td);
            Date ydDate = DateUtils.addDays(tdDate, -1);
            return simpleDateFormat.format(ydDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return null;
    }


}
