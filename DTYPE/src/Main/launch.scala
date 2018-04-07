package Main

import Data.{Read, Te}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext


import collection.JavaConverters._
/**
  * Created by Administrator on 2017/4/21.
  */
object launch {
  var BD : Array[(String,(String))] = _
  var BT : Array[(String,(String))] = _

  def main(args: Array[String]): Unit = {
   val sc = new SparkContext()
    /*val t = new Te()
    t.run(sc)*/
    //从Hbase中读取SA至今的关系

    Read.readMND()
    BD = Read.getMN_down.asScala
      .map(son=>(son.get(0),son.get(1))).toArray

    BT = Read.getMND_up.asScala
      .map(son=>(son.get(0),son.get(1))).toArray


    var BDRDD = sc.parallelize(BD)  //web
    var BTRDD = sc.parallelize(BT)  //db

    //MBTRDD.collect().foreach(println(_))

    /**
      * 生成 AM 之间的关系
      */
    BDRDD.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        myConf.set("hbase.zookeeper.property.clientPort","2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "DBWE")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{

            val p = new Put(Bytes.toBytes(item._1))

            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("TYPE"), Bytes.toBytes("web"))

            var page = Array("readpage","writepage")
            p.add(Bytes.toBytes("PAGE"), Bytes.toBytes("page_"+ (new util.Random).nextInt(100)  ), Bytes.toBytes(page((new util.Random).nextInt(2))))

            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }


    BTRDD.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        myConf.set("hbase.zookeeper.property.clientPort","2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "DBWE")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{

            val p = new Put(Bytes.toBytes(item._1))

            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("TYPE"), Bytes.toBytes("db"))

            var TABLE = Array("INSERT","DELETE","UPDATE","QUERY")
            p.add(Bytes.toBytes("TABLE"), Bytes.toBytes("TABLE_"+ (new util.Random).nextInt(100)  ), Bytes.toBytes(TABLE((new util.Random).nextInt(4))))
            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }
  }
}
