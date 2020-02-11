package com.atguigu.gmall0826.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;

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
             if(tableName.equals("order_info")&&eventType== CanalEntry.EventType.INSERT) {    // 下订单
                 for (CanalEntry.RowData rowData : rowDataList) {
                     List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                     for (CanalEntry.Column column : afterColumnsList) {
                         System.out.println(column.getName()+"::"+column.getValue());
                     }

                 }
             }
         }
    }


}
