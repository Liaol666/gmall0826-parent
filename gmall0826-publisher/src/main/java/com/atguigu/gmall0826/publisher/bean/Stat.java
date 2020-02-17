package com.atguigu.gmall0826.publisher.bean;


import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * 一个饼图
 */
@Data
@AllArgsConstructor
public class Stat {
    String title  ;

    List<Option> options;

}
