package Trigger

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/4/17.
  */
class te {
  var processname: String = _
  var runtime: Int = _

  var hour: Int = 8

  var burden: Int = _
  var dt: Int = 1
  def run(sc:SparkContext): Unit= {
    var A  = ArrayBuffer[(String)]()
    var i = 20000
    var j = 30000
    while(i <= j){
       var x = i + ""
       A += x
       i = i + 1
    }
    var SATNRDD = sc.parallelize(A)
    SATNRDD.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        //myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        //myConf.set("hbase.zookeeper.property.clientPort","2181")
        //myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "product")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{
            val p = new Put(Bytes.toBytes(item))
            p.add(Bytes.toBytes("milkinfo"), Bytes.toBytes("milkbatch"), Bytes.toBytes((item.toInt+100).toString))
            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }
  }
  println("===================================> constructed a trigger success <=====================================")
}
