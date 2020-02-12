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
             //下单业务
             if(tableName.equals("order_info")&&eventType== CanalEntry.EventType.INSERT) {    // 下订单
                 sendKafka(rowDataList, GmallConstant.KAFKA_TOPIC_ORDER);
             }
             //单据变更
             //用户新增。。
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
            String jsonString = jsonObject.toJSONString();
            MyKafkaSender.send(GmallConstant.KAFKA_TOPIC_ORDER,jsonString);
        }
    }
}
