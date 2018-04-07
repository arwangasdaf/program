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
   /* val t = new Te()
    t.run(sc)*/
    //从Hbase中读取SA至今的关系

    Read.readABC()
    BD = Read.getABC_DEFKEY.asScala
      .map(son=>(son.get(0),son.get(1))).toArray

    /*BT = Read.getABC_Type.asScala
      .map(son=>(son.get(0),son.get(1))).toArray*/


    var BDRDD = sc.parallelize(BD)
    /*var BTRDD = sc.parallelize(BT)*/

    /*var BNTRDD = BDRDD.join(BRDD)*/
    //MBTRDD.collect().foreach(println(_))

    /**
      *
      */
    BDRDD.foreachPartition{
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
            val p = new Put(Bytes.toBytes(item._2))

            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("ACTDEFID"), Bytes.toBytes(item._2 + ":1:100000007676"))

            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("DEFID"), Bytes.toBytes("1000000033336" + (new util.Random).nextInt(10000000)))
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("DEFTYPE"), Bytes.toBytes("flowchar"))
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("SYSTEM_ID"), Bytes.toBytes("1000000033336" + (new util.Random).nextInt(10000000)))

            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DOWN_"+ "flownode1" + item._2  ), Bytes.toBytes("flownode1" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DOWN_"+ "flownode2" + item._2  ), Bytes.toBytes("flownode2" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DOWN_"+ "flownode3" + item._2 ), Bytes.toBytes("flownode3" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DOWN_"+ "flownode4" + item._2 ), Bytes.toBytes("flownode4" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DOWN_"+ "flownode5" + item._2 ), Bytes.toBytes("flownode5" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DOWN_"+ "flownode6" + item._2 ), Bytes.toBytes("flownode6" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DOWN_"+ "flownode7" + item._2 ), Bytes.toBytes("flownode7" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DOWN_"+ "flownode8" + item._2 ), Bytes.toBytes("flownode8" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DOWN_"+ "flownode9"  + item._2 ), Bytes.toBytes("flownode9" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DOWN_"+ "flownode10" + item._2), Bytes.toBytes("flownode10" + item._2 ))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("DOWN_"+ "flownode10" + item._2), Bytes.toBytes("flownode10" + item._2 ))


            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }
  }
}
