package Read

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 2017/4/24.
  */
class readHbase {
  var config : Configuration = _
  var sc : SparkContext = _
  def this(config : Configuration , sc : SparkContext){
    this()
    this.config = config
    this.sc = sc
  }
  def read(table_name:String): RDD[(String,String,String,String)] = {

    config.set(TableInputFormat.INPUT_TABLE,table_name)
    // 用hadoopAPI创建一个RDD
    val hbaseRDD = sc.newAPIHadoopRDD(config, classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])

    val rowKeyRDD = hbaseRDD.map(tuple=>tuple._1).map(item=>Bytes.toString(item.get()))
    //rowKeyRDD.take(3).foreach(println)

    val resultRDD = hbaseRDD.map(tuple=>tuple._2.raw())
    //resultRDD.count()

    val testRdd = resultRDD.flatMap{res =>
      res.map(i => (Bytes.toString(i.getRow),Bytes.toString(i.getFamily),Bytes.toString(i.getQualifier),Bytes.toString(i.getValue)))}.cache()

    return testRdd
    //testRdd.collect().foreach(println(_))
  }
}
