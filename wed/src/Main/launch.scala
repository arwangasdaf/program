package Main

import java.util

import Data.Read
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, HTable, HTablePool, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

import collection.JavaConverters._
/**
  * Created by Administrator on 2017/4/21.
  */
object launch {
  var DB : Array[(String,(String,String))] = _
  var WEB : Array[(String,(String,String))] = _

  def main(args: Array[String]): Unit = {
    /*val t = new Te()
    t.run(sc)*/
    //从Hbase中读取SA至今的关系
    val sparkConf = new SparkConf().setAppName("HBaseTest").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //sparkConf.registerKryoClasses(Array(classOf[calculation],classOf[SATDN]))
    System.setProperty("spark.cores.max", "5")
    System.setProperty("spark.task.maxFailures", "8")
    System.setProperty("spark.akka.timeout", "300")
    System.setProperty("spark.network.timeout", "300")
    System.setProperty("spark.yarn.max.executor.failures", "100")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc) //创建sqlContext环境
    val config = HBaseConfiguration.create

    //计算线路的负载值的大小

    var map_AG = new util.HashMap[String, String]()
    val table_name_E = "EQUIPMENT"
    config.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
    config.set("hbase.zookeeper.property.clientPort", "2181")
    config.set("hbase.defaults.for.version.skip", "true")
    config.set(TableInputFormat.INPUT_TABLE, table_name_E)
    // 用hadoopAPI创建一个RDD
    //读取 SUBSYSTEM 表里的内容得到 SA 之间的关系
    val hbaseRDD_E = sc.newAPIHadoopRDD(config, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val resultRDD_E = hbaseRDD_E.map(tuple => tuple._2.raw())
    //resultRDD.count()

    val testRdd_E = resultRDD_E.flatMap { res =>
      res.map(i => (Bytes.toString(i.getRow), Bytes.toString(i.getQualifier)))
    }.cache()


    testRdd_E.collect().foreach {
      case item => {
        map_AG.put(item._2, item._1)
      }
    }



    val table_name_AG = "AG"
    config.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
    config.set("hbase.zookeeper.property.clientPort", "2181")
    config.set("hbase.defaults.for.version.skip", "true")
    config.set(TableInputFormat.INPUT_TABLE, table_name_AG)
    // 用hadoopAPI创建一个RDD
    //读取 SUBSYSTEM 表里的内容得到 SA 之间的关系
    val hbaseRDD_AG = sc.newAPIHadoopRDD(config, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val resultRDD_AG = hbaseRDD_AG.map(tuple => tuple._2.raw())
    //resultRDD.count()

    val testRdd_AG = resultRDD_AG.flatMap { res =>
      res.map(i => (Bytes.toString(i.getRow)))
    }.cache()


    val numRDD = sc.parallelize(1 to 9, 9).cache()
    val devRDD2 = testRdd_AG.cartesian(numRDD).cache()
    val fiaRDD = devRDD2.repartition(10000).cache()

    fiaRDD.foreachPartition {
      case triple => {
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        myConf.set("hbase.zookeeper.property.clientPort", "2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val pool = new HTablePool(myConf, 1000)
        val myTable = new HTable(myConf, "TEST1")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5 * 1024 * 1024)
        triple.foreach {
          case item => {
            val table_ZP = pool.getTable("ZP")
            val rowkey_ZP = item._1 + "_" + item._2.toString
            val get_ZP = new Get(rowkey_ZP.getBytes())
            val result_ZP = table_ZP.get(get_ZP)
            var map1 = new util.HashMap[String, Double]() //item -> Device
            for (kv1 <- result_ZP.raw()) {
              val Time_Value = Bytes.toString(kv1.getValue)
              val Time = Time_Value.split("_")(0)
              val Value = Time_Value.split("_")(1)
              val A = item._1.split("_")(0)
              val G1 = item._1.split("_")(1)
              val G2 = item._1.split("_")(2)


              val table = pool.getTable("ROUTE")
              val rowkey = A + "_" + G1 //A_source
              val rowkey_x = A + "_" + G2 //A_destination
              val start_dev = map_AG.get(rowkey)
              val end_dev = map_AG.get(rowkey_x)
              val start_end = start_dev + "_" + end_dev
              val get = new Get(start_end.getBytes())
              val result = table.get(get)
              for (kv <- result.raw()) {
                var route = Bytes.toString(kv.getQualifier)
                var routeTime = route + "_" + Time
                fput(map1, routeTime, Value.toDouble)
              }
            }

            for ((k: String, v: Double) <- map1) {

              //                               Devid  @ catkey  @     ft      @ fv  @ T
              val p = new Put(Bytes.toBytes(k))

              p.add(Bytes.toBytes("INFO"), Bytes.toBytes("VALUE"), Bytes.toBytes(v.toString))
              //
              myTable.put(p)
            }

            myTable.flushCommits()

          }
        }
      }
    }
  }

    /*var count = 0 ;
    resRDD1_ZP.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        myConf.set("hbase.zookeeper.property.clientPort","2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "ZP")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{
            // start_end

            val p = new Put(Bytes.toBytes(item._1+"_"+item._2+"_"+item._3+"_"+(count%8+1)))
            count = count + 1
            //INFO start_end
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("VALUE"), Bytes.toBytes(item._6+"_"+item._5))
            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }*/






  def fput(map: util.HashMap[String, Double], Key: String, Value: Double): Unit = {

    if (map.get(Key) != null) {
      map.put(Key, map.get(Key) + Value)
    } else {
      map.put(Key, Value)
    }
  }
}
