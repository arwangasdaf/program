package Main

import org.apache.spark.SparkContext
import Read.read
import Trigger.{Trigger, te}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import write.write

import collection.JavaConverters._
/**
  * Created by Administrator on 2017/4/16.
  */
object launch {
  var ATN:Array[(String, (String , String))] = _
  var A:Array[String] = _
  var SA : Array[(String,(String))] = _
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext();
   /* val e = new te()
    e.run(sc)*/
    read.readSA();
    SA = read.getSA().asScala
      .map(son=>(son.get(0),son.get(1))).toArray

    //对A集合去重
    A = SA.map{case (s , a) => (a)}.distinct
    var at: Int = 600
    var ab: Int = 3
    val trigger = new Trigger(A , at, ab)
    ATN = trigger.run().toArray

    val wri = new write(ATN)
    wri.run(sc)
    println("----------------------------finish write--------------------------------------")
   /* println("+++++++++++++++++++++++++")
        val myConf = HBaseConfiguration.create()
        /* myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
         myConf.set("hbase.zookeeper.property.clientPort","2181")
         myConf.set("hbase.defaults.for.version.skip", "true")*/

        val myTable = new HTable(myConf, "ATN1")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)

            val p = new Put(Bytes.toBytes("fdfd"))
            p.add(Bytes.toBytes("TIME"), Bytes.toBytes("9090"), Bytes.toBytes("erer"))
            myTable.put(p)
        println("********************")
        myTable.flushCommits()*/
  }
}
