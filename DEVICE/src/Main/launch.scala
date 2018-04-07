package Main

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HTable, HTablePool, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import Search.readHbase
/**
  * Created by Administrator on 2017/4/27.
  */
object launch{
  def main(args: Array[String]): Unit = {

    val config = HBaseConfiguration.create
    config.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
    config.set("hbase.zookeeper.property.clientPort","2181")
    config.set("hbase.defaults.for.version.skip", "true")
    val sparkConf = new SparkConf().setAppName("HBaseTest").set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    //读取 SUBSYSTEM 表里的内容得到 SA 之间的关系
    val tableBPM = "Ee"
    val sub = new readHbase(config , sc)
    val SAALLRDD = sub.read(tableBPM).map(item => (item._1 , item._2 , item._3 , item._4))
    //生成 SA 集合的RDD (S , A)               A                     G                          E                     T                 value
    val SARDD = SAALLRDD.map(item => (item._1.split("_")(0) , item._1.split("_")(1) , item._3.split("@")(0) , item._3.split("@")(1) , item._4)).cache()
    //计算 SpecjAppServer2004的会话操作次数
    val INRDD = SARDD.filter(item => (new String(item._3).equals("EJB_ATOM") && new String(item._2).equals("103"))).cache()
    val INRDD1 = INRDD.map(item => (item._1 , item._5.toDouble)).cache()
    //得到每个JOPS的一天总发生量
    val INZONGRDD = INRDD1.reduceByKey(_ + _).map(item => (item._1 , item._2/(600 * 43.7425))).cache()
    //INZONGRDD.collect().foreach(println(_))
    //得到上午最大JOPS的最大值 按照value排序
    val shangRDD = INRDD.filter(item => item._4.toDouble < 39270.0).map(item => (item._1 ,item._5)).groupByKey().cache()
    val shangRDD1 = shangRDD.map(item => (item._1,item._2.max)).cache()
    val xiaRDD = INRDD.filter(item => item._4.toDouble > 39270.0).map(item => (item._1 ,item._5)).groupByKey().cache()
    val xiaRDD1 = xiaRDD.map(item => (item._1,item._2.max)).cache()
    //得到                                                                A         n               上午最大      下午最大
    val INZONGRDD1 = INZONGRDD.join(shangRDD1).join(xiaRDD1).map(item=>(item._1 , item._2._1._1 , item._2._1._2 , item._2._2)).cache()


    //计算 SpecjAppServer2004的实体操作次数
    val INRDD_st = SARDD.filter(item => (new String(item._3).equals("EJB_ATOM") && new String(item._2).equals("104"))).cache()
    val INRDD1_st = INRDD.map(item => (item._1 , item._5.toDouble)).cache()
    //得到每个JOPS的一天总发生量
    val INZONGRDD3 = INRDD1_st.reduceByKey(_ + _).map(item => (item._1 , item._2/(600 * 43.7425))).cache()
    //INZONGRDD.collect().foreach(println(_))
    //得到上午最大JOPS的最大值 按照value排序
    //得到上午最大JOPS的最大值 按照value排序
    val shangRDD_st = INRDD_st.filter(item => item._4.toDouble < 39270.0).map(item => (item._1 ,item._5)).groupByKey().cache()
    val shangRDD1_st = shangRDD_st.map(item => (item._1,item._2.max)).cache()
    val xiaRDD_st = INRDD.filter(item => item._4.toDouble > 39270.0).map(item => (item._1 ,item._5)).groupByKey().cache()
    val xiaRDD1_st = xiaRDD.map(item => (item._1,item._2.max)).cache()
    //得到                                                                A         n               上午最大      下午最大
    val INZONGRDD1_st = INZONGRDD3.join(shangRDD1_st).join(xiaRDD1_st).map(item=>(item._1 , item._2._1._1 , item._2._1._2 , item._2._2)).cache()


    INZONGRDD1.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        //myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        //myConf.set("hbase.zookeeper.property.clientPort","2181")
        //myConf.set("hbase.defaults.for.version.skip", "true")
        val pool1 = new HTablePool(myConf, 1000)
        val myTable = pool1.getTable("Ff")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{
            val p = new Put(Bytes.toBytes(item._1))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("SpecjAppServer2004_huihua_avg"),Bytes.toBytes((item._2.toString.substring(0,4))))       //写入平均值
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("SpecjAppServer2004_huihua_max_1"),Bytes.toBytes((item._3.toString.substring(0,4))))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("SpecjAppServer2004_huihua_max_2"),Bytes.toBytes((item._4.toString.substring(0,4))))
            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }

    INZONGRDD1_st.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        //myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        //myConf.set("hbase.zookeeper.property.clientPort","2181")
        //myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "Ff")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{
            val p = new Put(Bytes.toBytes(item._1))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("SpecjAppServer2004_shiti_avg"),Bytes.toBytes((item._2.toString.substring(0,4))))       //写入平均值
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("SpecjAppServer2004_shiti_max_1"),Bytes.toBytes((item._3.toString.substring(0,4))))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("SpecjAppServer2004_shiti_max_2"),Bytes.toBytes((item._4.toString.substring(0,4))))
            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }

    //计算 TPCC
    TPCC.cal(SARDD , sc , config)
    byte.cal(SARDD , sc , config)
    KBP.cal(SARDD , sc , config)
    LinPack.cal(SARDD , sc , config)
    qingqiu.cal(SARDD , sc , config)
    J2EE.cal(SARDD , sc , config)
  }
}
