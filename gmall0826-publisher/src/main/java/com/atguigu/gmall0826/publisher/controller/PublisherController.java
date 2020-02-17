package com.atguigu.gmall0826.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0826.publisher.bean.Option;
import com.atguigu.gmall0826.publisher.bean.Result;
import com.atguigu.gmall0826.publisher.bean.Stat;
import com.atguigu.gmall0826.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.ws.rs.GET;
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

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date")String date,@RequestParam("startpage") int startpage,
            @RequestParam("size") int size,@RequestParam("keyword") String keyword){
        Map saleDetailResultMap = publisherService.getSaleDetail(date, keyword, startpage, size);
       //调整结构 比例 年龄段  中文
        Map ageAggsMap =(Map)saleDetailResultMap.get("ageAggsMap");
        Long total = (Long)saleDetailResultMap.get("total");
        //
        Long age_20Count=0L;
        Long age20_30Count=0L;
        Long age30_Count=0L;
        for (Object o : ageAggsMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String ageStr=(String) entry.getKey();
            Long ageCount= (Long) entry.getValue();
            int age=Integer.parseInt(ageStr);
            if(age<20){
                age_20Count+=ageCount;
            }else if(age>=20&&age<30){
                age20_30Count+=ageCount;
            }else {
                age30_Count+=ageCount;
            }

        }
        Double age_20rate= Math.round( age_20Count*1000D/total)/10D;
        Double age20_30rate=Math.round( age20_30Count*1000D/total)/10D;
        Double age30_rate=Math.round( age30_Count*1000D/total)/10D;
        List<Option> ageOptions=new ArrayList<>();
        ageOptions.add(new Option("20岁以下",age_20rate));
        ageOptions.add(new Option("20岁到30岁",age20_30rate));
        ageOptions.add(new Option("30岁及以上",age30_rate));


         //性别
        Map genderAggsMap =(Map)saleDetailResultMap.get("genderAggsMap");

        Long maleCount=(Long)genderAggsMap.get("M");
        Long femaleCount=(Long)genderAggsMap.get("F");
        Double maleRate=Math.round( maleCount*1000D/total)/10D;
        Double femaleRate=Math.round( femaleCount*1000D/total)/10D;
        List<Option> genderOptions=new ArrayList<>();
        genderOptions.add(new Option("男",maleRate));
        genderOptions.add(new Option("女",femaleRate));

        List<Stat> statList=new ArrayList<>();
        statList.add(new Stat("用户年龄占比",ageOptions) );
        statList.add(new Stat("用户性别占比",genderOptions) );

        List<Map> detail = (List<Map>)saleDetailResultMap.get("detail");
        Result result = new Result(total, detail, statList);
        return  JSON.toJSONString(result);
    }


}
