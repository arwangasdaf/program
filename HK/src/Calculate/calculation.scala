package Calculate

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Created by Administrator on 2017/4/25.
  */
class calculation {

  var SARDD:  RDD[(String , String)] = _
  var AMRDD:RDD[(String , String)] = _
  var BNRDD:RDD[(String , String)] = _
  var CDRDD:RDD[(String , String)] = _
  var MBRDD:RDD[(String , String)] = _
  var NCRDD:RDD[(String , String)] = _
  var dtypeRDD:RDD[(String , String)] = _
  var ABCDDTYPERDD:RDD[(String , (String , String , String , String))] = _
  var ATNRDD:RDD[(String , (String,String))] = _
  def this(SARDD:RDD[(String , String)] ,
           AMRDD:RDD[(String , String)] ,
           BNRDD:RDD[(String , String)] ,
           CDRDD:RDD[(String , String)] ,
           MBRDD:RDD[(String , String)] ,
           NCRDD:RDD[(String , String)] ,
           dtypeRDD:RDD[(String , String)] ,
           ATNRDD:RDD[(String , (String,String))]){
    this()
    this.SARDD = SARDD
    this.AMRDD = AMRDD
    this.BNRDD = BNRDD
    this.CDRDD = CDRDD
    this.MBRDD = MBRDD
    this.NCRDD = NCRDD
    this.dtypeRDD = dtypeRDD
    this.ATNRDD = ATNRDD
  }

  //计算函数
  def run(sc: SparkContext): Unit={
    // (m , a)
    val MARDD = AMRDD.map{case (a , m) => (m , a)}.cache()
    //(m , (a ,caozuotu)) => (b , a)
    val BARDD = MARDD.join(MBRDD).map{case (m , (a , b)) => (b , a)}.cache()
    //(b , (a , n)) => (n , (a , b))
    val NABRDD = BARDD.join(BNRDD).map{case (b , (a , n)) => (n , (a , b))}.cache()
    //(n , ((a,b) , c) => (c , (a, b))
    val CABRDD = NABRDD.join(NCRDD).map{case (n , ((a,b) , c)) => (c , (a, b))}.cache()
    //(c , ((a, b),D)) => (D , (a , b , c))
    val DABCRDD = CABRDD.join(CDRDD).map{case (c , ((a, b),d)) => (d , (a , b , c))}.cache()

    //(d , ((a , b , c) , web)) => (a , (b , c , d , web))
    this.ABCDDTYPERDD = DABCRDD.join(dtypeRDD).map{case (d , ((a , b , c) , dtype)) => (a , (b , c , d , dtype))}.cache()
    //(a , (b , c , d , dtype)) => ((a , dtype) , 1)
    val ADRDD = this.ABCDDTYPERDD.map{case (a , (b , c , d , dtype)) => ((a , dtype) , 1)}.cache()
    //((a , dtype) , 1) => ((a , dtype) , n)
    val ADNRDD = ADRDD.reduceByKey(_ + _).cache()
    //((a , dtype) , n) => (a , (dtype , n))
    val ADNNRDD = ADNRDD.map{case ((a , dtype) , n) => (a , (dtype , n))}.cache()
    //(s , a) => (a , s)
    val ASRDD = SARDD.map{case (s , a) => (a , s)}.cache()
    //(a , (s , (dtype , n))) => (a , (s , dtype , n))
    val ASDNRDD = ASRDD.join(ADNNRDD).map{case (a , (s , (dtype , n))) => (a , (s , dtype , n))}.cache()

    val SATNRDD = ASDNRDD.join(ATNRDD).cache()
    val finallRDD = SATNRDD.map{case (a , ((s , dtype , na),(t , nz))) => (s , a , t , dtype , (na * nz.toInt).toString)}
    /*println("------------------------------------------------------------------------------------")
    SATNRDD.collect().foreach(println(_))
    println("************************************************************************************")*/

    finallRDD.foreachPartition{
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
            val p = new Put(Bytes.toBytes(item._2+ "_" +item._1 + "_" +item._3))
            p.add(Bytes.toBytes("INFO"),Bytes.toBytes(item._4),Bytes.toBytes(item._5))

            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }
  }

  //得到 A 里的 d 的个数
  def get(): RDD[(String , (String , Int))] ={
    //(a , (b , c , d , dtype)) => ((a , d) , 1)
    var ADNUMRDD = this.ABCDDTYPERDD.map{case (a , (b , c , d , dtype)) => ((a , d) , 1)}
    //计算相同的 A 里的 d 的个数
    var NUMRDD = ADNUMRDD.reduceByKey(_ + _)
    //((a , d) , n) => (a , (d , n))
    var ADNRDD= NUMRDD.map{case ((a , d) , n) => (a , (d , n))}.cache()

    //返回(a , (d , n))
    return ADNRDD
  }
}
