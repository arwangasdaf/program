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

    Read.readABC()
    BD = Read.getABC_down.asScala
      .map(son=>(son.get(0),son.get(1))).toArray

    BT = Read.getABC_Type.asScala
      .map(son=>(son.get(0),son.get(1))).toArray


    var BDRDD = sc.parallelize(BD)
    var BTRDD = sc.parallelize(BT)

    var BNTRDD = BTRDD.join(BDRDD)
    //MBTRDD.collect().foreach(println(_))

    /**
      * 生成 AM 之间的关系
      */
    BNTRDD.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        //myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        //myConf.set("hbase.zookeeper.property.clientPort","2181")
        //myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "BPMDEFINITION_NODE")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{
            val p = new Put(Bytes.toBytes(item._2._2))

            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("SETID"), Bytes.toBytes((new util.Random).nextInt(100)))
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("DEFID"), Bytes.toBytes("1000" + (new util.Random).nextInt(100)))
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("NODETYPE"), Bytes.toBytes("operate"))
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("NODENAME"), Bytes.toBytes("100" + (new util.Random).nextInt(10)))
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("UP_DEFKEY"), Bytes.toBytes(item._1))

            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("DOWN_DEFKEY"), Bytes.toBytes("transaction1" + item._2._2))

            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }
  }
}
