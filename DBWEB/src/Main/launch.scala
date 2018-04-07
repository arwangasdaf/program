package Main

import java.util

import Read.readHbase
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.client.{Get, HTable, HTablePool, Put}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
/**
  * Created by Administrator on 2017/4/24.
  */
object launch {
  def main(args: Array[String]): Unit = {

    val config = HBaseConfiguration.create
    config.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
    config.set("hbase.zookeeper.property.clientPort","2181")
    config.set("hbase.defaults.for.version.skip", "true")
    val sparkConf = new SparkConf().setAppName("HBaseTest").set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.cores.max", "5")
    System.setProperty("spark.task.maxFailures", "8")
    System.setProperty("spark.akka.timeout", "300")
    System.setProperty("spark.network.timeout", "300")
    System.setProperty("spark.yarn.max.executor.failures", "100")
    val sc = new SparkContext(sparkConf)
    sparkConf.registerKryoClasses(Array(classOf[readHbase]))

    //读取 SUBSYSTEM 表里的内容得到 SA 之间的关系
    val tableBPM = "SATDN"
    val sub = new readHbase(config , sc)
    //                                                     A                   S                       T                     D            N
    val DBRDD = sub.read(tableBPM).map(item => (item._1.split("_")(0) , item._1.split("_")(1) ,item._1.split("_")(2) , item._3 , item._4)).repartition(500).cache() // (D , (A , n))


    DBRDD.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        //myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        //myConf.set("hbase.zookeeper.property.clientPort","2181")
        //myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "SATDN1")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{
            val p = new Put(Bytes.toBytes(item._1))   // A_T
            var a = Array("db" , "web" , "ftp" , "map" , "mail" , "Internet")
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("S"), Bytes.toBytes(item._2))
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("TIME"), Bytes.toBytes(item._3))
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("D_TYPE"), Bytes.toBytes(a(new (util.Random).nextInt(6))))
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("VALUE"), Bytes.toBytes(item._5))
            println("**********************************************************")
            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }
       //计算结束
    sc.stop()
  }
}


