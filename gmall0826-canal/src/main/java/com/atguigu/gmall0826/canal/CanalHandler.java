package com.atguigu.gmall0826.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall0826.canal.util.MyKafkaSender;
import com.atguigu.gmall0826.common.constant.GmallConstant;

import java.util.List;

public class CanalHandler {

    CanalEntry.EventType eventType;

    String tableName;

    List<CanalEntry.RowData> rowDataList;

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;
    }

    public void   handle(){
         if(this.rowDataList!=null &&this.rowDataList.size()>0){
             //下单业务主表
             if(tableName.equals("order_info")&&eventType== CanalEntry.EventType.INSERT) {    // 下订单
                 sendKafka(rowDataList, GmallConstant.KAFKA_TOPIC_ORDER);
             }else if(tableName.equals("order_detail")&&eventType== CanalEntry.EventType.INSERT){
                 sendKafka(rowDataList, GmallConstant.KAFKA_TOPIC_ORDER_DETAIL); //下单业务子表
             }else if(tableName.equals("user_info")&&(eventType== CanalEntry.EventType.INSERT||eventType== CanalEntry.EventType.UPDATE ) ){
                 sendKafka(rowDataList, GmallConstant.KAFKA_TOPIC_USER); //用户
             }

         }
    }

    public void sendKafka(List<CanalEntry.RowData>  rowDataList,  String topic){
        for (CanalEntry.RowData rowData : rowDataList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                System.out.println(column.getName()+"::"+column.getValue());
                jsonObject.put(column.getName(),column.getValue());
            }
            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String jsonString = jsonObject.toJSONString();
            MyKafkaSender.send(topic,jsonString);
        }
    }
}
