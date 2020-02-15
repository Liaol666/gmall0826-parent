package com.atguigu.gmall0826.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0826.common.constant.GmallConstant
import com.atguigu.gmall0826.realtime.bean.{AlertInfo, EventInfo, StartupLog}
import com.atguigu.gmall0826.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._
object AlertApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("alertApp")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_EVENT,ssc)
    //
    //inputDstream.map(_.value()).print()
    val eventInfoDstream: DStream[EventInfo] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val eventInfo: EventInfo = JSON.parseObject(jsonString, classOf[EventInfo])

      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
      val datetimeString: String = dateFormat.format(new Date(eventInfo.ts))
      val datetimeArr: Array[String] = datetimeString.split(" ")
      eventInfo.logDate = datetimeArr(0)
      eventInfo.logHour = datetimeArr(1)
      eventInfo
    }

//    拆
//    1 同一设备   groupbykey
//    2 5分钟内    window
//    3 三次及以上用不同账号登录并领取优惠劵   //  对操作集合进行判断
//      4 并且在登录到领劵过程中没有浏览商品
//
//    5 按照日志的格式进行保存  //调整结构

    // 窗口大小 决定了数据范围   滑动步长决定 数据更新频率
    val windowDstream: DStream[EventInfo] = eventInfoDstream.window(Seconds(300),Seconds(5))
   //分组
    val eventInfoWithMidDstream: DStream[(String, EventInfo)] = windowDstream.map(eventInfo=>(eventInfo.mid,eventInfo))
    val groupbyMidDstream: DStream[(String, Iterable[EventInfo])] = eventInfoWithMidDstream.groupByKey()
   // 筛选日志
    val alertInfoDstream: DStream[(Boolean, AlertInfo)] = groupbyMidDstream.map { case (mid, eventInfoItr) =>
      //三次及以上用不同账号登录并领取优惠劵  =》 判断领取优惠券的时候 使用的账号超过了3个
      val uidSet = new util.HashSet[String]()
      val itemSet = new util.HashSet[String]()
      val eventList = new util.ArrayList[String]()
      var ifClickItem = false   //看是否在过程中有浏览商品
      breakable(
        for (eventInfo: EventInfo <- eventInfoItr) {
          eventList.add(eventInfo.evid)
          if (eventInfo.evid == "coupon") {
            uidSet.add(eventInfo.uid)
            itemSet.add(eventInfo.itemid)
          }
          if (eventInfo.evid == "clickItem") {
            ifClickItem = true
            break()
          }
        }
      )
      val ifAlert: Boolean = uidSet.size() >= 3 && !ifClickItem
      (ifAlert, AlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))
    }

    val alertInfoFilteredDstream: DStream[AlertInfo] = alertInfoDstream.filter(_._1).map(_._2)

    //alertInfoFilteredDstream.print(100)

    alertInfoFilteredDstream.foreachRDD{ rdd=>
      rdd.foreachPartition{ alertInfoItr=>
        //为了达到通过id进行保存 同时利用数据库的幂等性去重 所以
        // 每分钟相同的mid只保存一条  =》 id = mid + 分钟TS
        val sourceList: List[(String, AlertInfo)] = alertInfoItr.toList.map(alertInfo => (alertInfo.mid + "_" + alertInfo.ts / 1000 / 60, alertInfo))
        MyEsUtil.insertBulk(sourceList,GmallConstant.ES_INDEX_ALERT)
      }


    }





    ssc.start()
    ssc.awaitTermination()
  }

}
