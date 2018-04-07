import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/5/11.
  */
object launch {
    def main(args: Array[String]) {
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

      testRdd.foreachPartition{
        triple => {
          val myConf = HBaseConfiguration.create()
          myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
          myConf.set("hbase.zookeeper.property.clientPort","2181")
          myConf.set("hbase.defaults.for.version.skip", "true")
          val myTable = new HTable(myConf, "Ff")
          myTable.setAutoFlush(false, false)
          myTable.setWriteBufferSize(5*1024*1024)
          triple.foreach{
            item=>{
              val p = new Put(Bytes.toBytes(item._1))
              p.add(Bytes.toBytes("ITEM"),Bytes.toBytes("SYSTEMID"),Bytes.toBytes(((new util.Random).nextInt(45)+1).toString))
              myTable.put(p)
            }
          }
          myTable.flushCommits()
        }
      }
    }
}
