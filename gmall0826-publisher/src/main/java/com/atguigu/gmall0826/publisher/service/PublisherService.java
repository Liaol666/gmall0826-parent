package com.atguigu.gmall0826.publisher.service;

import java.util.Map;

public interface PublisherService {

    public Long getDauTotal(String date);

    public Map getDauHourCount(String date);

    public Double getOrderAmount(String date);

    public Map getOrderAmountHour(String date);
}
