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
  var processname: String = _
  var runtime: Int = _

  var hour: Int = 8

  var burden: Int = _
  var dt: Int = 1
  def run(sc:SparkContext): Unit= {
    var A  = ArrayBuffer[(String,String,String,String,String,String,String,String,String,String,String,String)]()
    var i = 0
    var j = 10000
    while(i <= j){
      var rowkey = (10000000+i) + ""
      var yuanchandi = "150000"
      var jinzhong = 30
      var shougouriqi = "2016-10-3"
      var shougouyuan = "liwu"
      var suandu = 13
      var suandushebie = "110"+i
      var xijun = 60
      var xijunshebei = "1111"+i
      var andingdu = 9
      var andindushebei =  i + "3399"
      var jianceyuan = "yujili"
      var event = (rowkey,yuanchandi,jinzhong.toString,shougouriqi,shougouyuan,suandu.toString,suandushebie,xijun.toString,xijunshebei,andingdu.toString,andindushebei,jianceyuan)
      A += event
      i = i + 1
    }
    var SATNRDD = sc.parallelize(A.map(x=>(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10,x._11,x._12)))
    SATNRDD.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        //myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        //myConf.set("hbase.zookeeper.property.clientPort","2181")
        //myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "sourcemilk")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{
            val p = new Put(Bytes.toBytes(item._1))

            p.add(Bytes.toBytes("milkinfo"), Bytes.toBytes("sourcearea"), Bytes.toBytes(item._2))

            p.add(Bytes.toBytes("milkinfo"), Bytes.toBytes("weight"), Bytes.toBytes(item._3))

            p.add(Bytes.toBytes("milkinfo"), Bytes.toBytes("stockdata"), Bytes.toBytes(item._4))

            p.add(Bytes.toBytes("milkinfo"), Bytes.toBytes("stocker"), Bytes.toBytes(item._5))

            p.add(Bytes.toBytes("milkcheck"), Bytes.toBytes("acidity"), Bytes.toBytes(item._6))

            p.add(Bytes.toBytes("milkcheck"), Bytes.toBytes("equipment"), Bytes.toBytes(item._7))

            p.add(Bytes.toBytes("milkcheck"), Bytes.toBytes("germ"), Bytes.toBytes(item._8))

            p.add(Bytes.toBytes("milkcheck"), Bytes.toBytes("gequipment"), Bytes.toBytes(item._9))

            p.add(Bytes.toBytes("milkcheck"), Bytes.toBytes("constancy"), Bytes.toBytes(item._10))

            p.add(Bytes.toBytes("milkcheck"), Bytes.toBytes("cequipment"), Bytes.toBytes(item._11))

            p.add(Bytes.toBytes("milkcheck"), Bytes.toBytes("Tetman"), Bytes.toBytes(item._12))

            myTable.put(p)
          }

        }
        myTable.flushCommits()
      }
    }
}

  println("===================================> constructed a trigger success <=====================================")
}
