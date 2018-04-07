package Main

import java.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.client.{Get, HTable, HTablePool, Put}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
/**
  * Created by Administrator on 2017/4/24.
  */
object launch {
  def main(args: Array[String]): Unit = {

    //spark环境变量,采用kryo序列化
    val sparkConf = new SparkConf().setAppName("HBaseTest").set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //sparkConf.registerKryoClasses(Array(classOf[calculation],classOf[SATDN]))
    System.setProperty("spark.cores.max", "5")
    System.setProperty("spark.task.maxFailures", "8")
    System.setProperty("spark.akka.timeout", "300")
    System.setProperty("spark.network.timeout", "300")
    System.setProperty("spark.yarn.max.executor.failures", "100")
    val sc = new SparkContext(sparkConf)


    val config = HBaseConfiguration.create
    val table_name = "SUBSYSTEM"
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

    val SAALLRDD = testRdd.filter(item => new String(item._2).equals("ITEM")).repartition(440)
    // item._1  S   item._4  A

    SAALLRDD.foreachPartition{
      case triple => {

        //获得系统当前时间
        val t0 = System.nanoTime : Double
        //获得一次Hbase数据库连接服务
        val myConf = HBaseConfiguration.create()
        //获得一次create的时间差
        println((System.nanoTime() - t0).toDouble / 1000000)



        myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        myConf.set("hbase.zookeeper.property.clientPort", "2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val pool1 = new HTablePool(myConf, 1000)
        val table = pool1.getTable("BPMDEFINITION")
        val pool = new HTablePool(myConf, 1000)
        val table1 = pool.getTable("BPMDEFINITION_NODE")
        var pool3 = new HTablePool(myConf , 1000)
        val table_ATN = pool3.getTable("ATN")
        var pool2 = new HTablePool(myConf , 1000)
        val pool5 = new HTablePool(myConf, 1000)
        val table5 = pool5.getTable("DBWE")
        val myTable = pool2.getTable("ADN1")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          case item =>{
            // item._1  S   item._4  A
            //创建一个 Hashmap
            //创建一个 Hashmap 用来存储 (D , n)
            var dbmap = new util.HashMap[String , Int]()
            var webmap = new util.HashMap[String , Int]()
            var rowkey = item._4    //查找 A
            val get1 = new Get(rowkey.getBytes())
            val result = table.get(get1)
            for (am <- result.raw()) {
              var qual = Bytes.toString(am.getFamily)  //得到列族
              if(qual.equals("ITEM")){              //得到流程图下的流程节点
              var M = Bytes.toString(am.getValue)
                //根据 M 到 BPMDEFINITION_NODE 表里查找 M 对应的操作图
                var rowkey_1 = M
                var get2 = new Get(rowkey_1.getBytes())
                var result_1 = table1.get(get2)
                for(mc <- result_1.raw()){
                  //找到 m 对应的操作图
                  if(Bytes.toString(mc.getFamily).equals("INFO") && Bytes.toString(mc.getQualifier).equals("DOWN_DEFKEY")){
                    var operate = Bytes.toString(mc.getValue)  //得到 m 对应的操作图
                    //根据 b 对应的操作图去找对应的 n
                    var rowkey_2 = operate
                    var get3 = new Get(rowkey_2.getBytes())
                    var result_2 = table.get(get3)
                    for(bn <- result_2.raw()){
                      //找到  b(操作图) 包含的 n
                      var qual_1 = Bytes.toString(bn.getFamily)  //得到 Item 为列族
                      if(qual_1.equals("ITEM")){
                        var n = Bytes.toString(bn.getValue)   //得到 n (操作图节点)
                        //根据 n 找 对应的事务图
                        var rowkey_3 = n
                        var get4 = new Get(rowkey_3.getBytes())
                        var result_3 = table1.get(get4)
                        for(nc <- result_3.raw()){
                          if(Bytes.toString(mc.getFamily).equals("INFO") && Bytes.toString(mc.getQualifier).equals("DOWN_DEFKEY")){
                            var transaction = Bytes.toString(nc.getValue)
                            //根据 c (事务图) 去找对应的 事务图节点 d
                            var rowkey_4 = transaction
                            var get5 = new Get(rowkey_4.getBytes())
                            var result_4 = table.get(get5)
                            for(cd <- result_4.raw()){
                              var qual_2 = Bytes.toString(cd.getFamily)
                              if(qual_2.equals("ITEM")){
                                var d = Bytes.toString(cd.getValue)
                                // 根据每一个 d 去找相应的 d.type
                                var rowkey_5 = d
                                var get6 = new Get(rowkey_5.getBytes())
                                var result_5 = table1.get(get6)
                                var result_6 = table5.get(get6)
                                for(ddtype <- result_5.raw()){
                                  if(Bytes.toString(ddtype.getQualifier).equals("NODE_SERVICE_TYPE")){
                                    //得到对应的dtype (db/web)
                                    var dtype = Bytes.toString(ddtype.getValue)
                                    if(new String(dtype).equals("db")){
                                      for(dtable <- result_6.raw()) {
                                        var table = new String(dtable.getQualifier)
                                        var insert = new String(dtable.getValue)
                                        fput(dbmap, table , 1)
                                      }
                                    }
                                    if(new String(dtype).equals("web")){
                                      for(dtable <- result_6.raw()) {
                                        var table = new String(dtable.getQualifier)
                                        var insert = new String(dtable.getValue)
                                        fput(webmap, table , 1)
                                      }
                                    }
                                    //将 (A_S , D , D.type) 写入 Hbase 里 , 表名为 ASDD
                                    /*myTable.setAutoFlush(false, false)
                                    myTable.setWriteBufferSize(5*1024*1024)
                                    val p = new Put(Bytes.toBytes(item._4))
                                    p.add(Bytes.toBytes("INFO"), Bytes.toBytes(d), Bytes.toBytes(dtype))
                                    myTable.put(p)*/
                                    // myTable.flushCommits()

                                    //var dtype = Bytes.toString(ddtype.getValue)
                                    // item._1  S   item._4  A
                                    //将 D.type 1 放入 map 中

                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }

            //每一个 S A 计算结束 , 然后 根据 ATN 集合 算出来 (S , A , T , d.type , n)
            for ((k: String, v: Int) <- dbmap) {
              if(!k.equals("TYPE")) {
                val p = new Put(Bytes.toBytes(item._4))
                p.add(Bytes.toBytes("DB"), Bytes.toBytes(k), Bytes.toBytes(v.toString))
                myTable.put(p)
              }
            }
            for ((k: String, v: Int) <- webmap) {
              if(!k.equals("TYPE")) {
                val p = new Put(Bytes.toBytes(item._4))
                p.add(Bytes.toBytes("WEB"), Bytes.toBytes(k), Bytes.toBytes(v.toString))
                myTable.put(p)
              }
            }
          }
        }
        myTable.flushCommits()
      }
    }

    //计算结束
    sc.stop()
  }

  //定义一个 Hashmap 累加函数
  def fput(map: util.HashMap[String, Int], Key: String, Value: Int): Unit = {

    if (map.get(Key) != null) {
      map.put(Key, map.get(Key) + Value)
    } else {
      map.put(Key, Value)
    }
  }
}
