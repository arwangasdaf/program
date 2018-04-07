package Data

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/4/18.
  */
class Te {
  def run(sc:SparkContext): Unit= {
    var A  = ArrayBuffer[(String,String)]()
    var i = 1000
    var j = 1043
    while(i <= j){
      var zixitong = ""+i
      var typ = ""+(i-1000)
      var event = (zixitong , typ)
      A += event
      i = i + 1
    }
    var SATNRDD = sc.parallelize(A.map(x => (x._1,x._2)))
    SATNRDD.collect().foreach(println(_))
    SATNRDD.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        //myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        //myConf.set("hbase.zookeeper.property.clientPort","2181")
        //myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "SUBSYSTEM")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{
            val p = new Put(Bytes.toBytes(item._2))

            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("SYSTEM_NAME"), Bytes.toBytes(item._2))

            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("SYSTEM_TYPE"), Bytes.toBytes((new util.Random).nextInt(10)))

            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DEFKEY_"+ "flowchar1" + item._2  ), Bytes.toBytes("flowchar1" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DEFKEY_"+ "flowchar2" + item._2  ), Bytes.toBytes("flowchar2" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DEFKEY_"+ "flowchar3" + item._2 ), Bytes.toBytes("flowchar3" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DEFKEY_"+ "flowchar4" + item._2 ), Bytes.toBytes("flowchar4" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DEFKEY_"+ "flowchar5" + item._2 ), Bytes.toBytes("flowchar5" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DEFKEY_"+ "flowchar6" + item._2 ), Bytes.toBytes("flowchar6" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DEFKEY_"+ "flowchar7" + item._2 ), Bytes.toBytes("flowchar7" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DEFKEY_"+ "flowchar8" + item._2 ), Bytes.toBytes("flowchar8" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DEFKEY_"+ "flowchar9"  + item._2 ), Bytes.toBytes("flowchar9" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DEFKEY_"+ "flowchar10" + item._2), Bytes.toBytes("flowchar10" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DEFKEY_"+ "flowchar11" + item._2), Bytes.toBytes("flowchar11" + item._2 ))

            myTable.put(p)
          }

        }
        myTable.flushCommits()
      }
    }
}

  println("===================================> constructed a trigger success <=====================================")
}
