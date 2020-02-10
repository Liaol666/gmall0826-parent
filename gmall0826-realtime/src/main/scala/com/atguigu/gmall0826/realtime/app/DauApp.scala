package com.atguigu.gmall0826.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0826.common.constant.GmallConstant
import com.atguigu.gmall0826.realtime.bean.StartupLog
import com.atguigu.gmall0826.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

object DauApp {
//  pv  uv dau
//  //page visit   页面访问
//  user visit  用户访问    日  周  月 季度 年

  // daily active user   每日活跃用户   user 注册用户  ip地址  mid 设备id


  def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")

        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)
         //recordDstream.map(_.value()).print()
    // 1 进行一个格式转换 补充时间字段
    val startUpLogDstream: DStream[StartupLog] = recordDstream.map { record =>
      val jsonString: String = record.value()
      val startupLog: StartupLog = JSON.parseObject(jsonString, classOf[StartupLog])

      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
      val datetimeString: String = dateFormat.format(new Date(startupLog.ts))
      val datetimeArr: Array[String] = datetimeString.split(" ")
      startupLog.logDate = datetimeArr(0)
      startupLog.logHour = datetimeArr(1)

      startupLog
    }



    //2  去重   保留每个mid当日的第一条   其他的启动日志过滤掉

    //  然后再利用清单进行过滤筛选 把清单中已有的用户的新日志过滤掉
/* 方案A     连接redis次数太多  可以进一步优化
 val filteredDstream: DStream[StartupLog] = startUpLogDstream.filter { startuplog =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val dauKey = "dau:" + startuplog.logDate
      val flag = !jedis.sismember(dauKey, startuplog.mid)
      jedis.close()
      flag
    }*/
    // 优化：  利用driver查询出完整清单，然后利用广播变量发送给各个executor
    // 各个ex  利用广播变量中的清单  检查自己的数据是否需要过滤 在清单中的一律清洗掉
    //1  查 driver
  /*  方案B 错误代码  会造成driver只执行了一次  不会周期性执行
    val jedis: Jedis = RedisUtil.getJedisClient
    val today: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    val dauKey="dau:"+today
    val midSet: util.Set[String] = jedis.smembers(dauKey)
    jedis.close()
  // 2  发
    val midBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)

    startUpLogDstream.filter{startuplog=>  //3 ex 收   筛查
      val midSet: util.Set[String] = midBC.value
      !midSet.contains(startuplog.mid)
    }*/

    //方案C
    //让driver 每5秒执行一次
    val filteredDstream: DStream[StartupLog] = startUpLogDstream.transform { rdd =>
      //driver 每5秒执行一次
      //1  查 driver
      println("过滤前：" + rdd.count())
      val jedis: Jedis = RedisUtil.getJedisClient
      val today: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val dauKey = "dau:" + today
      val midSet: java.util.Set[String] = jedis.smembers(dauKey)
      jedis.close()
      // 2  发
      val midBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)

      val filteredRDD: RDD[StartupLog] = rdd.filter { startuplog =>
        //3 ex 收   筛查
        val midSet: util.Set[String] = midBC.value
        !midSet.contains(startuplog.mid)
      }
      println("过滤后：" + filteredRDD.count())
      filteredRDD
      // driver
    }

    //  自检内部  分组去重 ： 0 转换成kv  1   先按mid进行分组   2 组内排序 按时间排序  3  取 top1
    val groupbyMidDstream: DStream[(String, Iterable[StartupLog])] = filteredDstream.map(startuplog=>(startuplog.mid,startuplog)).groupByKey()

    val realFilteredDstream: DStream[StartupLog] = groupbyMidDstream.map { case (mid, startuplogItr) =>
      val startuplogList: List[StartupLog] = startuplogItr.toList.sortWith { (startuplog1, startuplog2) =>
        startuplog1.ts < startuplog2.ts // 正序排序
      }
      val top1startupLogList: List[StartupLog] = startuplogList.take(1)
      top1startupLogList(0)
    }


    realFilteredDstream.cache()

    //  利用redis保存当日访问过的用户清单
    realFilteredDstream.foreachRDD { rdd =>
      rdd.foreachPartition { startupLogItr =>
        val jedis: Jedis = RedisUtil.getJedisClient
        // val jedis = new Jedis("hadoop1",6379)   // ex
        for (startupLog <- startupLogItr) {
          val dauKey = "dau:" + startupLog.logDate
          // type  set     // key    dau:2020-02-08     //value  mid
          println(startupLog)
          jedis.sadd(dauKey, startupLog.mid)
          jedis.expire(dauKey, 60 * 60 * 24);
        }
        jedis.close()

      }
    }
      realFilteredDstream.foreachRDD{rdd=>
        rdd.saveToPhoenix("GMALL0826_DAU",
          Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS")
        ,new Configuration,Some("hadoop1,hadoop2,hadoop3:2181"))

      }




         ssc.start()
         ssc.awaitTermination()


  }



}
