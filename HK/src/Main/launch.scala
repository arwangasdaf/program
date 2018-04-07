package Main

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import Read.readHbase
import Calculate.calculation
import Calculate.SATDN
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.spark.storage.StorageLevel
/**
  * Created by Administrator on 2017/4/24.
  */
object launch {
  def main(args: Array[String]): Unit = {

    //spark环境变量,采用kryo序列化
    val config = HBaseConfiguration.create
    config.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
    config.set("hbase.zookeeper.property.clientPort","2181")
    config.set("hbase.defaults.for.version.skip", "true")
    val sparkConf = new SparkConf()
                   .setAppName("HBaseTest")
                   .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[calculation]))
    System.setProperty("spark.task.maxFailures", "8")
    System.setProperty("spark.akka.timeout", "300")
    System.setProperty("spark.network.timeout", "300")
    System.setProperty("spark.yarn.max.executor.failures", "100")
    val sc = new SparkContext(sparkConf)

    //读取 SUBSYSTEM 表里的内容得到 SA 之间的关系
    val tableBPM = "SUBSYSTEM"
    val sub = new readHbase(config , sc)
    val SAALLRDD = sub.read(tableBPM).map(item => (item._1 , (item._2 , item._3 , item._4)))
        .filter(item => new String(item._2._1).equals("ITEM"))

    //生成 SA 集合的RDD (S , A)
    val SARDD = SAALLRDD.map(item => (item._1 , item._2._3)).cache()

    //读取 BPMDEFINITON 表里的内容 得到 A B C 和绑定节点之间的关系
    val tableNode = "BPMDEFINITION"
    val SAT = new readHbase(config , sc)
    val ABCALLRDD = SAT.read(tableNode).map(item => (item._1 , (item._2 , item._3 , item._4)))
          .cache()
    //得到所有类型为 flowchar 的图 (A , flowchar)
    val ATYPERDD = ABCALLRDD.map(item => (item._1 , item._2._3))
      .filter(item => new String(item._2).equals("flowchar"))
    //得到所有 A down 的集合RDD
    val ABCDOWNRDD = ABCALLRDD.filter{case item => new String(item._2._1).equals("ITEM")}
            .cache()
    //得到 AM 的集合 (A , M)
    val AMRDD = ATYPERDD.join(ABCDOWNRDD)
      .map{case (a , (typ , (item , down , downnode))) => (a , downnode)}.cache()

   //得到所有类型为 operate 的图 (B , operate)
    val BTYPERDD = ABCALLRDD.map(item => (item._1 , item._2._3))
      .filter(item => new String(item._2).equals("operate"))
    //得到 BN 的集合 (B , N)
    val BNRDD = BTYPERDD.join(ABCDOWNRDD)
      .map{case (b , (typ , (item , down , downnode))) => (b , downnode)}.cache()

    //得到所有类型为 transaction 的图 (C , transaction)
    val CTYPERDD = ABCALLRDD.map(item => (item._1 , item._2._3))
      .filter(item => new String(item._2).equals("transaction"))
    //得到 cd 的集合 (c ， d)
    val CDRDD = CTYPERDD.join(ABCDOWNRDD)
      .map{case (c , (typ , (item , down , downnode))) => (c , downnode)}.cache()


    //读取 BPMDEFINITON_NODE 表里的内容 得到 绑定节点 和 BC以及Dtype之间的关系
    val table = "BPMDEFINITION_NODE"
    val BPN = new readHbase(config , sc)
    val MNVALLRDD = BPN.read(table).map(item => (item._1 , (item._2 , item._3 , item._4)))
    //得到所有MN的down (m , (info , down_defkey , cuozuotu))
    val MNVDOWNRDD = MNVALLRDD.filter(item => new String(item._2._2).equals("DOWN_DEFKEY"))
    //得到所有类型为 flowchar 的图 (m , (info , nodetype , typ)) => (M , flowchar)
    val MTYPERDD = MNVALLRDD.map(item => (item._1 , item._2._2 , item._2._3))
      .filter(item => new String(item._3).equals("flowchar"))
      .map(item => (item._1 , item._3)).cache()
    //得到 M 的集合 (m , caozuotu)
    val MBRDD = MTYPERDD.join(MNVDOWNRDD)
      .map{case (m , (typ , (item , down , downnode))) => (m , downnode)}.cache()


    //得到所有类型为 operate 的图 (N , operate)
    val NTYPERDD = MNVALLRDD.map(item => (item._1 , item._2._2 , item._2._3))
      .filter(item => new String(item._3).equals("operate"))
      .map(item => (item._1 , item._3)).cache()
    //得到 M 的集合 (n , shiwutu)
    val NCRDD = NTYPERDD.join(MNVDOWNRDD)
      .map{case (n , (typ , (item , down , downnode))) => (n , downnode)}.cache()

    //得到所有类型为 transaction 的图 (d , db/web)
    val typeRDD = MNVALLRDD.filter(item => new String(item._2._2).equals("NODE_SERVICE_TYPE"))
    val dTYPERDD = MNVALLRDD.map(item => (item._1 , item._2._2 , item._2._3))
      .filter(item => new String(item._3).equals("transaction"))
      .map(item => (item._1 , item._3)).cache()
    //得到 M 的集合 (d , db/web/ftp)
    val dtypeRDD = dTYPERDD.join(typeRDD)
      .map{case (d , (typ , (info , nodeservice , webf))) => (d , webf)}.cache()

    /*
      * 取出 (A , T , N) 集合
      * */
    val tableA = "ATN1"
    val atn = new readHbase(config , sc)
    //广播ATN集合
    val ATNRDD = atn.read(tableA).map(item => (item._1 , (item._3 , item._4)))
    //val ATNRDD = atn.read(tableA).map(item => (item._1 , (item._3 , item._4))).cache()


    /*
    * 调用计算模块进行计算  (S , A , T , Dtype , n)
    * 返回 (a , (s , dtype , n))
    * */
    val cal =  new calculation( SARDD , AMRDD , BNRDD , CDRDD , MBRDD , NCRDD , dtypeRDD ,ATNRDD)
    cal.run(sc)



    sc.stop()
  }
}
