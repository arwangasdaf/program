package write

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/4/16.
  */
class write {
  var ATN:Array[(String, (String , String))] = _
  def this(ATN:Array[(String, (String , String))]) {
    this()
    this.ATN = ATN;
  }
  def run(sc:SparkContext): Unit = {
      var SATNRDD = sc.parallelize(ATN)
      SATNRDD.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
       /* myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        myConf.set("hbase.zookeeper.property.clientPort","2181")
        myConf.set("hbase.defaults.for.version.skip", "true")*/
        val myTable = new HTable(myConf, "ATN1")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{
            val p = new Put(Bytes.toBytes(item._1))
            p.add(Bytes.toBytes("TIME"), Bytes.toBytes(item._2._1), Bytes.toBytes(item._2._2))
            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }
  }
  println("===================================> write information <=====================================")
}
