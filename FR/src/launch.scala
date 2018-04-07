package Main

import java.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.client.{Get, HTable, HTablePool, Put}
import org.apache.hadoop.hbase.thrift2.generated.THBaseService.Processor.put
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
/**
  * Created by Administrator on 2017/4/24.
  */
object launch {
  def main(args: Array[String]): Unit = {

    //spark环境变量,采用kryo序列化
    val sparkConf = new SparkConf().setAppName("HBaseTest").set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //sparkConf.registerKryoClasses(Array(classOf[calculation],classOf[SATDN]))
    System.setProperty("spark.cores.max", "5")
    System.setProperty("spark.task.maxFailures", "8")
    System.setProperty("spark.akka.timeout", "300")
    System.setProperty("spark.network.timeout", "300")
    System.setProperty("spark.yarn.max.executor.failures", "100")
    val sc = new SparkContext(sparkConf)

    //var rdd = sc.textFile(args(0)).repartition(40).cache()
   val config = HBaseConfiguration.create
    val table_name = "SUBSYSTEM"
    config.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
    config.set("hbase.zookeeper.property.clientPort","2181")
    config.set("hbase.defaults.for.version.skip", "true")
    config.set(TableInputFormat.INPUT_TABLE,table_name)
    // 用hadoopAPI创建一个RDD
    //读取 SUBSYSTEM 表里的内容得到 SA 之间的关系
    val hbaseRDD = sc.newAPIHadoopRDD(config, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val resultRDD = hbaseRDD.map(tuple=>tuple._2.raw())
    //resultRDD.count()

    val testRdd = resultRDD.flatMap{res =>
      res.map(i => (Bytes.toString(i.getRow),Bytes.toString(i.getFamily),Bytes.toString(i.getQualifier),Bytes.toString(i.getValue)))}.cache()

    val ATNrdd = testRdd.filter(item => new String(item._2).equals("ITEM")).repartition(500).cache()


    /*val config = HBaseConfiguration.create
    val table_name = "ATN"
    config.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
    config.set("hbase.zookeeper.property.clientPort","2181")
    config.set("hbase.defaults.for.version.skip", "true")
    config.set(TableInputFormat.INPUT_TABLE,table_name)
    // 用hadoopAPI创建一个RDD
    //读取 SUBSYSTEM 表里的内容得到 SA 之间的关系
    val hbaseRDD = sc.newAPIHadoopRDD(config, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val resultRDD = hbaseRDD.map(tuple=>tuple._2.raw())
    //resultRDD.count()

    val testRdd = resultRDD.flatMap{res =>
      res.map(i => (Bytes.toString(i.getRow),Bytes.toString(i.getFamily),Bytes.toString(i.getQualifier),Bytes.toString(i.getValue)))}.cache()

    val ATNrdd = testRdd.map(item => (item._1,(item._3,item._4))) //(A , T  ,N)

    val table = "ADN"
    config.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
    config.set("hbase.zookeeper.property.clientPort","2181")
    config.set("hbase.defaults.for.version.skip", "true")
    config.set(TableInputFormat.INPUT_TABLE,table)
    // 用hadoopAPI创建一个RDD
    //读取 SUBSYSTEM 表里的内容得到 SA 之间的关系
    val hbaseRDD1 = sc.newAPIHadoopRDD(config, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val resultRDD1 = hbaseRDD1.map(tuple=>tuple._2.raw())
    //resultRDD.count()

    val testRdd1 = resultRDD1.flatMap{res =>
      res.map(i => (Bytes.toString(i.getRow),Bytes.toString(i.getFamily),Bytes.toString(i.getQualifier),Bytes.toString(i.getValue)))}.cache()

    val ADNrdd = testRdd1.map(item => (item._1,(item._3,item._4))) //(A , D  ,N)

    val fiRDD = ATNrdd.join(ADNrdd).map(item => (item._1 , item._2._1._1 , item._2._1._2.toInt * item._2._2._2.toInt , item._2._2._1  )).cache()

    fiRDD.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        //myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        //myConf.set("hbase.zookeeper.property.clientPort","2181")
        //myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "dbn")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{
            val p = new Put(Bytes.toBytes(item._1 + "_" + item._2))   // A_T

            p.add(Bytes.toBytes("INFO"), Bytes.toBytes(item._4), Bytes.toBytes(item._3))

            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }*/



    ATNrdd.foreachPartition{
      case triple => {
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        myConf.set("hbase.zookeeper.property.clientPort","2181")
        myConf.set("hbase.defaults.for.version.skip", "true")

        var pool2 = new HTablePool(myConf , 1000)
        val myTable = pool2.getTable("dbn")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)

        val myTable1= pool2.getTable("webn")
        myTable1.setAutoFlush(false, false)
        myTable1.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          case item =>{


            val pool5 = new HTablePool(myConf, 1000)
            val pool6 = new HTablePool(myConf, 1000)


            val table_ATN = pool6.getTable("ATN1")
            // item._1  S   item._4  A
            //创建一个 Hashmap
            //创建一个 Hashmap 用来存储 (D , n)
            //var puts = new  util.ArrayList[Put]()

            var rowkey = item._4   //查找 A
            val get1 = new Get(rowkey.getBytes())
           // var nano= System.nanoTime()
            val result = table_ATN.get(get1)

            /*val p1 = new Put(Bytes.toBytes(item+"11111111"))  //A_T
            //p1.add(Bytes.toBytes("INFO"),Bytes.toBytes("TIME"),Bytes.toBytes(((System.nanoTime() - nano).toDouble / 1000000).toString))
            myTable.put(p1)*/

            val table_ADN = pool5.getTable("ADN1")
            val get3 = new Get(rowkey.getBytes())
            var result3 = table_ADN.get(get3)

            for (am <- result.raw()) {

              var T = Bytes.toString(am.getQualifier) //得到列族
              var n = Bytes.toString(am.getValue).toInt

                for (kv <- result3.raw()) {
                  var table = Bytes.toString(kv.getQualifier)
                  var num = Bytes.toString(kv.getValue).toInt * n
                  val p = new Put(Bytes.toBytes(item._4))
                    if (Bytes.toString(kv.getFamily).equals("DB")) {
                      //A
                      p.add(Bytes.toBytes("INFO"),Bytes.toBytes("TIME"),Bytes.toBytes(T))
                      p.add(Bytes.toBytes("INFO"),Bytes.toBytes("ACTION"),Bytes.toBytes(table))
                      p.add(Bytes.toBytes("INFO"),Bytes.toBytes("VALUE"),Bytes.toBytes(num.toString))
                      myTable.put(p)
                  }
                  if (Bytes.toString(kv.getFamily).equals("WEB")) {
                    p.add(Bytes.toBytes("INFO"),Bytes.toBytes("TIME"),Bytes.toBytes(T))
                    p.add(Bytes.toBytes("INFO"),Bytes.toBytes("ACTION"),Bytes.toBytes(table))
                    p.add(Bytes.toBytes("INFO"),Bytes.toBytes("VALUE"),Bytes.toBytes(num.toString))
                    myTable1.put(p)
                  }
                }
            }
            }
            myTable.flushCommits()
            myTable1.flushCommits()
            }
          }
    }

    //计算结束
    sc.stop()
  }

}
