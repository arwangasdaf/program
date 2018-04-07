package main

import java.lang.annotation.Annotation
import java.util
import java.util.Map.Entry
import java.util.Properties

import Data.Read
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import zzz_koloboke_compile.shaded.com.$google$.common.base.Equivalence
import com.koloboke.compile
import com.koloboke.compile.KolobokeMap
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

import collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable
/**
  * Created by Administrator on 2017/4/28.
  */
object launch {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseTest").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //sparkConf.registerKryoClasses(Array(classOf[calculation],classOf[SATDN]))
    sparkConf.set("spark.scheduler.mode", "FAIR")
    System.setProperty("spark.cores.max", "5")
    System.setProperty("spark.task.maxFailures", "8")
    System.setProperty("spark.akka.timeout", "300")
    System.setProperty("spark.network.timeout", "300")
    System.setProperty("spark.yarn.max.executor.failures", "100")
    val sc = new SparkContext(sparkConf)


    var map_D_TYPE = new util.HashMap[String, String]()
    val config = HBaseConfiguration.create
    val table_name = "DEVICE"
    config.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
    config.set("hbase.zookeeper.property.clientPort", "2181")
    config.set("hbase.defaults.for.version.skip", "true")
    config.set(TableInputFormat.INPUT_TABLE, table_name)
    // 用hadoopAPI创建一个RDD
    //读取 SUBSYSTEM 表里的内容得到 SA 之间的关系
    val hbaseRDD = sc.newAPIHadoopRDD(config, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val resultRDD = hbaseRDD.map(tuple => tuple._2.raw())
    //resultRDD.count()

    val testRdd = resultRDD.flatMap { res =>
      res.map(i => (Bytes.toString(i.getRow), Bytes.toString(i.getQualifier)))
    }.cache()

    testRdd.collect().foreach {
      case item => {
        map_D_TYPE.put(item._1, item._2)
      }
    }


    val map = new util.HashMap[String, String]()
    val table_namee = "h_nod_A01_B01"
    config.set(TableInputFormat.INPUT_TABLE, table_namee)
    // 用hadoopAPI创建一个RDD
    //读取 SUBSYSTEM 表里的内容得到 SA 之间的关系
    val hbaseRDDe = sc.newAPIHadoopRDD(config, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val resultRDDe = hbaseRDDe.map(tuple => tuple._2.raw())
    //resultRDD.count()

    val testRdde = resultRDDe.flatMap { res =>
      res.map(i => (Bytes.toString(i.getRow), Bytes.toString(i.getQualifier)))
    }.cache()

    testRdde.collect().foreach {
      case item => {
        map.put(item._1, item._2)
      }
    }

    val sqlContext = new SQLContext(sc) //创建sqlContext环境
    /*val jdbcMap = Map("url" -> "jdbc:oracle:thin:@//172.16.0.210:1521/ORADB6",
      "user" -> "SFDADB",
      "password" -> "isms2017",
      "dbtable" -> "T_IMS_DEV_LOG",
      "driver" -> "oracle.jdbc.driver.OracleDriver")
    val jdbcDF = sqlContext.read.options(jdbcMap).format("jdbc").load
    jdbcDF.registerTempTable("DEV")


    val map = new util.HashMap[String, String]()
    val resRDD = sqlContext.sql("select DEV.DEV_CODE,DEV.LOAD_PROP from DEV").rdd.cache()
    val resRDD1 = resRDD.map(line => (line.get(0).toString, line.get(1).toString)).distinct().cache()

    resRDD1.collect().foreach {
      case item => {
        map.put(item._1, item._2)
      }
    }*/

    //查 T_IMS_SHOW_BUILD1 => (D , FT , Value)
    val jdbcMap1 = Map("url" -> "jdbc:oracle:thin:@//172.16.0.210:1521/ORADB6",
      "user" -> "SFDADB",
      "password" -> "isms2017",
      "dbtable" -> "T_IMS_SHOW_BUILD1",
      "driver" -> "oracle.jdbc.driver.OracleDriver")
    val jdbcDF1 = sqlContext.read.options(jdbcMap1).format("jdbc").load
    jdbcDF1.registerTempTable("D_FT_V")




    var map_D_FT_V = new util.HashMap[String, String]()
    val resRDD2 = sqlContext.sql("select D_FT_V.DEVICE_ID,D_FT_V.LOAD_TYPE,D_FT_V.REF_VALUES from D_FT_V").rdd.cache()
    val resRDD3 = resRDD2.map(line => (line.get(0).toString, line.get(1).toString, line.get(2).toString)).cache()
    resRDD3.collect().foreach {
      case item => {
        map_D_FT_V.put(item._1 + "_" + item._2, item._3)
      }
    }


    var map_file = new util.HashMap[String, String]()
    val jdbcMapq = Map("url" -> "jdbc:oracle:thin:@//172.16.0.210:1521/ORCL",
      "user" -> "isms2",
      "password" -> "isms2017",
      "dbtable" -> "IBPS_FILE_ATTACHMENT",
      "driver" -> "oracle.jdbc.driver.OracleDriver")
    val jdbcDFq = sqlContext.read.options(jdbcMapq).format("jdbc").load
    jdbcDFq.registerTempTable("file")


    val resRDDq = sqlContext.sql("select file.ID_,file.FILE_PATH_ from file").rdd
    val resRDDp = resRDDq.map(line => (line.get(0).toString, line.get(1).toString)).cache()
    resRDDp.collect().foreach {
      case item => {
        map_file.put(item._1, item._2)
      }
    }





    var devRdd = sc.textFile(args(0))
    val devRDD1 = devRdd.flatMap(line => line.split(" ")).cache()
    val numRDD = sc.parallelize(1 to 9, 9).cache()
    val devRDD2 = devRDD1.cartesian(numRDD).cache() //(dev , num) =>(00 , 1)
    val xrdd = devRDD2.repartition(200).cache()

    xrdd.foreachPartition { case triple => {
      val myConf = HBaseConfiguration.create()
      myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
      myConf.set("hbase.zookeeper.property.clientPort", "2181")
      myConf.set("hbase.defaults.for.version.skip", "true")
      val pool1 = new HTablePool(myConf, 1000)

      val pool = new HTablePool(myConf, 1000)

      val poolD = new HTablePool(myConf, 1000)

      val myTable = new HTable(myConf, "H")
      myTable.setAutoFlush(false, false)
      myTable.setWriteBufferSize(5 * 1024 * 1024)
      triple.foreach {
        case item => {
          val table = pool1.getTable("LOAD")
          val table1 = pool.getTable("EQUIPMENT")
          val table2 = pool.getTable("h_balancea_A01_B01")

          var map1 = new util.HashMap[String, Double]() //item -> Device
          var rowkey = item._1
          val get1 = new Get(rowkey.getBytes())

          val result = table1.get(get1)


          for (kv <- result.raw()) {
            val A_G_N = Bytes.toString(kv.getQualifier) + "_" + item._2.toString //A_G
            val get2 = new Get(A_G_N.getBytes())

            val result1 = table.get(get2)
            for (kv1 <- result1.raw()) {
              var FT_T = Bytes.toString(kv1.getQualifier) //T_FT
              var fv = Bytes.toString(kv1.getValue)
              fput(map1, FT_T, fv.toDouble)
            }
          }
          for ((k: String, v: Double) <- map1) {
            if (map_D_FT_V.containsKey(item._1 + "_" + k.substring(0, k.length - 9))) {

              var num = v * map.get(item._1).toDouble
              var fuzai = k.substring(0, k.length - 9)
              val get3 = new Get((item._1 + "@" + fuzai).getBytes())

              val resultx = table2.get(get3)

              if (resultx.getExists()) {
                for (kv <- resultx.raw()) {
                  var li = Bytes.toString(kv.getValue)
                  var liyonglv = num * li.toDouble / map_D_FT_V.get(item._1 + "_" + fuzai).toDouble
                  var Type = map_D_TYPE.get(item._1)
                  var rowkey = Type + "@" + fuzai
                  val table_D = poolD.getTable("DEV_ACTION")
                  val get1 = new Get(rowkey.getBytes())
                  val result = table_D.get(get1)
                  for (kv <- result.raw()) {
                    var limit_max_level_id = Bytes.toString(kv.getQualifier)
                    var limit = limit_max_level_id.split("@")(0).toDouble
                    var max = limit_max_level_id.split("@")(1).toDouble
                    var level = limit_max_level_id.split("@")(2)
                    var Id = limit_max_level_id.split("@")(3)
                    var filename = map_file.get(Id)
                    var T = (k.substring(k.length - 8, k.length - 3).toDouble / 12).toString.substring(0, 4)
                    if (liyonglv >= limit && liyonglv <= max) {
                      //                               Devid  @ catkey  @     ft      @ fv  @ T
                      val p = new Put(Bytes.toBytes(T.toString + "@" + item._1 + "@" + Type + "@" + k.substring(0, k.length - 9) + "@" + num))

                      p.add(Bytes.toBytes("INFO"), Bytes.toBytes(filename + "@" + level), Bytes.toBytes(Bytes.toString(kv.getValue) + "@" + Id))
                      //
                      myTable.put(p)
                    }
                  }
                }

              }


            }
            myTable.flushCommits()
          }
        }
      }

    }

    }

    def fput(map: util.HashMap[String, Double], Key: String, Value: Double): Unit = {

      if (map.get(Key) != null) {
        map.put(Key, map.get(Key) + Value)
      } else {
        map.put(Key, Value)
      }
    }
  }
}
