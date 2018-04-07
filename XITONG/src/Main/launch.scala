package Main

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/6/2.
  */
object launch {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDToMysql").setMaster("local")
    val sc = new SparkContext(conf)
    val config = HBaseConfiguration.create
    val table_name = "Ff"
    config.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
    config.set("hbase.zookeeper.property.clientPort","2181")
    config.set("hbase.defaults.for.version.skip", "true")
    config.set(TableInputFormat.INPUT_TABLE,table_name)
    // 用hadoopAPI创建一个RDD
    //读取 SUBSYSTEM 表里的内容得到 SA 之间的关系
    val hbaseRDD = sc.newAPIHadoopRDD(config, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val resultRDD = hbaseRDD.map(tuple=>tuple._2.raw())
    //resultRDD.count()

    val testRdd = resultRDD.flatMap{res =>
      res.map(i => (Bytes.toString(i.getRow),Bytes.toString(i.getFamily),Bytes.toString(i.getQualifier),Bytes.toString(i.getValue)))}.cache()

    //                                                                           A        S
    val ASRDD = testRdd.filter(item=> item._3.equals("SYSTEMID")).map(item=>(item._1 , item._4)).cache()
    //                                                                             A        BYTE_avg       v
    val BYTE1RDD = testRdd.filter(item=> !item._3.equals("SYSTEMID")).map(item=>(item._1 , (item._3, item._4))).cache()
    //                                                  S        BYTE_avg
    val BYTE2RDD = ASRDD.join(BYTE1RDD).map(item=>((item._2._1 , item._2._2._1),item._2._2._2.toDouble))
    val BYTE3RDD = BYTE2RDD.reduceByKey(_ + _).cache()


    BYTE3RDD.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        myConf.set("hbase.zookeeper.property.clientPort","2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "zixitong")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{
            val p = new Put(Bytes.toBytes(item._1._1))
            p.add(Bytes.toBytes("ITEM"),Bytes.toBytes(item._1._2),Bytes.toBytes(item._2.toString))
            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }
  }
}
