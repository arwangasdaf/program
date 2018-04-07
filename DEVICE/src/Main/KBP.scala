package Main

import Search.readHbase
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 2017/5/12.
  */
object KBP {
  def cal(SARDD:RDD[(String,String,String,String,String)], sc:SparkContext , config:Configuration)={
    //计算 SpecjAppServer2004的会话操作次数
    val INRDD = SARDD.filter(item => (new String(item._3).equals("RTS")))
    val INRDD1 = INRDD.map(item => (item._1 , item._4.toDouble))
    //得到每个JOPS的一天总发生量
    val INZONGRDD = INRDD1.reduceByKey(_ + _).map(item => (item._1 , item._2)).cache()
    //INZONGRDD.collect().foreach(println(_))
    //得到上午最大JOPS的最大值 按照value排序
    val shangRDD = INRDD.filter(item => item._4.toDouble < 39270.0).map(item => (item._1 ,item._5)).groupByKey().cache()
    val shangRDD1 = shangRDD.map(item => (item._1,item._2.max)).cache()
    val xiaRDD = INRDD.filter(item => item._4.toDouble > 39270.0).map(item => (item._1 ,item._5)).groupByKey().cache()
    val xiaRDD1 = xiaRDD.map(item => (item._1,item._2.max)).cache()
    //得到                                                                A         n               上午最大      下午最大
    val INZONGRDD1 = INZONGRDD.join(shangRDD1).join(xiaRDD1).map(item=>(item._1 , item._2._1._1 , item._2._1._2 , item._2._2)).cache()

    INZONGRDD1.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        //myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        //myConf.set("hbase.zookeeper.property.clientPort","2181")
        //myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "Ff")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{
            val p = new Put(Bytes.toBytes(item._1))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("RTS_avg"),Bytes.toBytes(item._2.toString.substring(0,4)))       //写入平均值
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("RTS_max_1"),Bytes.toBytes((item._3.toString.substring(0,4))))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("RTS_max_2"),Bytes.toBytes((item._4.toString.substring(0,4))))
            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }
  }
}