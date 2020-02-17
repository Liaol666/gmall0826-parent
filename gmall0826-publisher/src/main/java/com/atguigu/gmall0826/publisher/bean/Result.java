package com.atguigu.gmall0826.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * 灵活查询的总结果集
 */
@Data
@AllArgsConstructor
public class Result {
    Long total;
    List<Map>  detail;
    List<Stat>  stat;
}
