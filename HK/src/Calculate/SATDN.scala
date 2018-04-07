package Calculate


import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 2017/4/25.
  */
class SATDN {
  //(a , (s , dtype , n))
  var ASDNARDD:RDD[(String , (String , String , Int))] = _
  //(a , t , n)
  var ATNRDD: RDD[(String , (String , String))] = _
  def this(ASDNARDD:RDD[(String , (String , String , Int))],
            ATNRDD: RDD[(String , (String , String))])
  {
    this()
    this.ASDNARDD = ASDNARDD
    this.ATNRDD = ATNRDD
  }

  /*
  * 计算 (S , A , T , N)
  * */
  def cal(): Unit ={
    //(a , ((s , dtype , n),(t , n))
    val SATNRDD = ASDNARDD.join(ATNRDD)
      .map{case (a , ((s , dtype , na),(t , nz))) => (s , a , t , dtype , (na * nz.toInt).toString)}
    /*println("------------------------------------------------------------------------------------")
    SATNRDD.collect().foreach(println(_))
    println("************************************************************************************")*/

    SATNRDD.foreachPartition{
      triple => {
        //myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        //myConf.set("hbase.zookeeper.property.clientPort","2181")
        //myConf.set("hbase.defaults.for.version.skip", "true")
        val myConf = HBaseConfiguration.create()
        val myTable = new HTable(myConf, "TEST")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{
            val p = new Put(Bytes.toBytes(item._2 + "_" +item._1 + "_" +item._3))
            p.add(Bytes.toBytes("INFO"),Bytes.toBytes(item._4),Bytes.toBytes(item._5))

            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }
  }
}
