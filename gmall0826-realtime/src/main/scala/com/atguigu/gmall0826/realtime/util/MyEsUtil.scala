package com.atguigu.gmall0826.realtime.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyEsUtil {


  private val ES_HOST = "http://hadoop1"
  private val ES_HTTP_PORT = 9200
  private var factory:JestClientFactory = null

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if ( client!=null) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)

  }


  def  insertBulk(sourceList:List[(String ,Any)],indexName:String ): Unit ={
    if(sourceList!=null&&sourceList.size>0){
      val jest: JestClient = getClient
      val bulkBuilder = new Bulk.Builder
      for ((id,source) <- sourceList ) {
        //1  先构造单词插入 2 把单次插入汇总起来 一起提交
        val index: Index = new Index.Builder(source).index(indexName).`type`("_doc").id(id).build()
        bulkBuilder.addAction(index)
      }

      val bulk: Bulk = bulkBuilder.build()
      val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulk).getItems
      println("保存了："+items.size()+"条数据")
      jest.close()
    }
  }


  def main(args: Array[String]): Unit = {
      val jest: JestClient = getClient
      val index: Index = new Index.Builder(Customer0826("zhang3",33)).index("customer_0826").`type`("ctype").id("1").build()
      jest.execute(index)
      jest.close()
  }

  case class  Customer0826(name:String ,age:Int)


}
