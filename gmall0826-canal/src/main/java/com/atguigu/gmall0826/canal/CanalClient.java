package com.atguigu.gmall0826.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

    public static void main(String[] args) {
          // 1  连接canal的server端
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop1", 11111), "example", "", "");

        while(true){
            canalConnector.connect();
            //  2   抓取数据
            canalConnector.subscribe("*.*");
            // 1个message对象 包含了100个什么？
            // 100个SQL单位     1个SQL单位 = 一个sql执行后影响的row集合
            Message message = canalConnector.get(100);
            if(message.getEntries().size()==0){
                System.out.println("没有数据，休息一会！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{  //有数据  entry==SQL单位
                //  3   把抓取到的数据展开，变成咱们想要的格式
                for (CanalEntry.Entry entry : message.getEntries()) {
                    if(CanalEntry.EntryType.ROWDATA== entry.getEntryType()){
                        ByteString storeValue = entry.getStoreValue();
                        //rowchange是结构化的,反序列化的sql单位
                        CanalEntry.RowChange rowChange=null;
                        try {
                              rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        //得到行集合
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        //得到操作类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        // 得到表名
                        String tableName = entry.getHeader().getTableName();

                        //   4   根据不同业务类型发送到不同的kafka
                        CanalHandler canalHandler = new CanalHandler(eventType, tableName, rowDatasList);
                        canalHandler.handle();

                    }



                }





            }








        }







    }

}
