package Main


import java.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, HTablePool, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable
/**
  * Created by Administrator on 2017/5/3.
  */
object launch1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application") //给Application命名
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc);
    val jdbcMap_ZP = Map("url" -> "jdbc:oracle:thin:@//172.16.0.210:1521/ORADB6",
      "user" -> "SFDADB",
      "password" -> "isms2017",
      "dbtable" -> "T_IMS_SPLIT_ROUTER",
      "driver" -> "oracle.jdbc.driver.OracleDriver")
    val jdbcDF_ZP = sqlContext.read.options(jdbcMap_ZP).format("jdbc").load
    jdbcDF_ZP.registerTempTable("DEV")

    val t0 = System.nanoTime : Double

    val resRDD = sqlContext.sql("select DEV.SOURCE_RES,DEV.OBJ_RES,DEV.S_SRC_POINT,DEV.S_DEST_POINT,DEV.SEQUENCE from DEV").rdd.cache()

    resRDD.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        myConf.set("hbase.zookeeper.property.clientPort","2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "ROUTE")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{
            // start_end
            val p = new Put(Bytes.toBytes(item.get(0).toString+"_"+item.get(1).toString))
            //INFO start_end
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes(item.get(2).toString+"_"+item.get(3).toString), Bytes.toBytes(item.get(4).toString))
            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }



    sc.stop()
  }

}
