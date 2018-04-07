package Main

import Data.{Read, Te}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

import collection.JavaConverters._
/**
  * Created by Administrator on 2017/4/21.
  */
object launch {
  var BD : Array[(String,(String))] = _
  var BT : Array[(String,(String))] = _

  def main(args: Array[String]): Unit = {
   val sc = new SparkContext()
    /*val t = new Te()
    t.run(sc)*/
    //从Hbase中读取SA至今的关系

    Read.readMND()
    BD = Read.getMN_down.asScala
      .map(son=>(son.get(0),son.get(1))).toArray

    BT = Read.getMND_up.asScala
      .map(son=>(son.get(0),son.get(1))).toArray


    var BDRDD = sc.parallelize(BD)
    var BTRDD = sc.parallelize(BT)
    //得到所有的操作图B的名称
    var BNTRDD = BTRDD.join(BDRDD)

    /**
      * 生成 AM 之间的关系
      */
    BNTRDD.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        //myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        //myConf.set("hbase.zookeeper.property.clientPort","2181")
        //myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "BPMDEFINITION")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{
            val p = new Put(Bytes.toBytes(item._2._2))

            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("ACTDEFID"), Bytes.toBytes((new util.Random).nextInt(100)))

            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("DEFID"), Bytes.toBytes("100" + (new util.Random).nextInt(100)))
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("DEFTYPE"), Bytes.toBytes("operate"))

            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DOWN_"+ "OperateNode1" + item._2._2  ), Bytes.toBytes("OperateNode1" + item._2._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DOWN_"+ "OperateNode2" + item._2._2  ), Bytes.toBytes("OperateNode2" + item._2._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DOWN_"+ "OperateNode3" + item._2._2 ), Bytes.toBytes("OperateNode3" + item._2._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DOWN_"+ "OperateNode4" + item._2._2 ), Bytes.toBytes("OperateNode4" + item._2._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DOWN_"+ "OperateNode5" + item._2._2 ), Bytes.toBytes("OperateNode5" + item._2._2 ))

            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }
  }
}
