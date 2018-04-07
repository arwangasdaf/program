package Main

import java.util.Properties

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by Administrator on 2017/5/24.
  */
object launch {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MySQL")
    System.setProperty("spark.cores.max", "5")
    System.setProperty("spark.task.maxFailures", "8")
    System.setProperty("spark.akka.timeout", "300")
    System.setProperty("spark.network.timeout", "300")
    System.setProperty("spark.yarn.max.executor.failures", "100")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)    //创建sqlContext环境
    //定义 jdbc 连接
    val url = "jdbc:mysql://172.16.0.141:3306/bpmx157?user=user124&password=123"
    val prop = new Properties()
    //将 MySQL 表转换成 dataFrame
    val df = sqlContext.read.jdbc(url, "W_SUBSYSTEMDEF", prop)
    df.show()
    //将 dataFrame 注册成表
    df.registerTempTable("SUBSYSTEM")
    //对 dataFrame 进行 Sql 语句处理拿到子系统和流程作业的关系
    val SUBRDD = sqlContext.sql("select SUBSYSTEM.F_sys_id,SUBSYSTEM.F_sys_defkey from SUBSYSTEM").rdd
    //转换生成(子系统S , 流程作业A)的RDD tuple
    val SUBSYSTEMRDD = SUBRDD.map(line => (line.get(0).toString,line.get(1).toString)).cache()
    SUBSYSTEMRDD.foreachPartition{
      triple => {
        //myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        //myConf.set("hbase.zookeeper.property.clientPort","2181")
        //myConf.set("hbase.defaults.for.version.skip", "true")
        val myConf = HBaseConfiguration.create()
        val myTable = new HTable(myConf, "SUBSYSTEM1")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{
            val p = new Put(Bytes.toBytes(item._1))
            p.add(Bytes.toBytes("INFO"),Bytes.toBytes("SYSTEM_NAME"),Bytes.toBytes(item._1))
            p.add(Bytes.toBytes("ITEM"),Bytes.toBytes("DEKFEY_"+item._2),Bytes.toBytes(item._2))
            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }


    val df3 = sqlContext.read.jdbc(url, "bpm_definition1", prop)
    df3.show()
    //将 dataFrame 注册成表afd
    df3.registerTempTable("bpm_definition")
    //对 dataFrame 进行 Sql 语句处理拿到子系统和流程作业的关系
    val flowcharRDD = sqlContext.sql("select bpm_definition.ACTDEFID,bpm_definition.DEFID,bpm_definition.DEFTYPE,bpm_definition.DOWN,bpm_definition.DEFKEY from bpm_definition where bpm_definition.DEFTYPE = 'flowchar'").rdd
    //转换生成(子系统S , 流程作业A)的RDD tuple
    val fRDD1 = flowcharRDD.map(line => (line.get(0).toString,line.get(1).toString,line.get(2).toString,line.get(3).toString,line.get(4).toString)).cache()
    fRDD1.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        val myTable = new HTable(myConf, "BPMDEFINITION1")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        myConf.set("hbase.zookeeper.property.clientPort","2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        triple.foreach{
          item=>{
            val p = new Put(Bytes.toBytes(item._5))
            p.add(Bytes.toBytes("INFO"),Bytes.toBytes("ACTDEFID"),Bytes.toBytes(item._1))
            p.add(Bytes.toBytes("INFO"),Bytes.toBytes("DEFID"),Bytes.toBytes(item._2))
            p.add(Bytes.toBytes("INFO"),Bytes.toBytes("DEFTYPE"),Bytes.toBytes(item._3))
            p.add(Bytes.toBytes("ITEM"),Bytes.toBytes("DOWN_"+item._4),Bytes.toBytes(item._4))
            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }





    val df5 = sqlContext.read.jdbc(url, "bpm_node_set1", prop)
    df5.show()
    //将 dataFrame 注册成表
    df5.registerTempTable("bpm_node_set")
    //对 dataFrame 进行 Sql 语句处理拿到子系统和流程作业的关系
    val flowcharnodeRDD = sqlContext.sql("select bpm_node_set.DEFID,bpm_node_set.NODENAME,bpm_node_set.NODETYPE,bpm_node_set.SETID,bpm_node_set.UP_DEFKEY,bpm_node_set.DOWN_DEFKEY,bpm_node_set.NODE from bpm_node_set").rdd
    //转换生成(子系统S , 流程作业A)的RDD tuple
    val ffRDD1 = flowcharnodeRDD.map(line => (line.get(0).toString,line.get(1).toString,line.get(2).toString,line.get(3).toString,line.get(4).toString,line.get(5).toString,line.get(6).toString)).cache()
    ffRDD1.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        val myTable = new HTable(myConf, "BPMDEFINITION_NODE1")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        myConf.set("hbase.zookeeper.property.clientPort","2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        triple.foreach{
          item=>{
            val p = new Put(Bytes.toBytes(item._7))
            p.add(Bytes.toBytes("INFO"),Bytes.toBytes("DEFID"),Bytes.toBytes(item._1))
            p.add(Bytes.toBytes("INFO"),Bytes.toBytes("NODENAME"),Bytes.toBytes(item._2))
            p.add(Bytes.toBytes("INFO"),Bytes.toBytes("NODETYPE"),Bytes.toBytes(item._3))
            p.add(Bytes.toBytes("INFO"),Bytes.toBytes("SETID"),Bytes.toBytes(item._4))
            p.add(Bytes.toBytes("INFO"),Bytes.toBytes("UP_DEFKEY"),Bytes.toBytes(item._5))
            p.add(Bytes.toBytes("INFO"),Bytes.toBytes("DOWN_DEFKEY"),Bytes.toBytes(item._6))

            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }

  }
}
