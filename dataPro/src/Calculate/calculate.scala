package Calculate

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

/**
  * Created by Administrator on 2017/4/15.
  */
class calculate {
  var SA : Array[(String,(String))] = _
  var ABC_version : Array[(String , (String))] = _
  var ABC_Type : Array[(String , (String))] = _
  var ABC_down : Array[(String , (String))] = _
  var MN_down : Array[(String , (String))] = _
  var MND_up : Array[(String , (String))] = _
  var D_Type : Array[(String , (String))] = _
  var D_Type_db : Array[(String , (String))] = _
  var D_Type_web : Array[(String , (String))] = _
  var ATNX : Array[(String , (String , String))] = _

  def this(SA : Array[(String,(String))] ,
  ABC_version : Array[(String , (String))] ,
  ABC_Type : Array[(String , (String))] ,
  ABC_down : Array[(String , (String))] ,
  MN_down : Array[(String , (String))] ,
  MND_up : Array[(String , (String))] ,
  D_Type : Array[(String , (String))] ,
  D_Type_db : Array[(String , (String))] ,
  D_Type_web : Array[(String , (String))] ,
  ATNX : Array[(String , (String , String))])
  {
    this()
    this.SA = SA
    this.ABC_version = ABC_version
    this.ABC_Type = ABC_Type
    this.ABC_down = ABC_down
    this.MN_down = MN_down
    this.MND_up = MND_up
    this.D_Type = D_Type
    this.D_Type_db = D_Type_db
    this.D_Type_web = D_Type_web
    this.ATNX = ATNX
  }

  def run(sc: SparkContext): Unit ={
    //(S1 , A1)
    val SARDD = sc.parallelize(SA)

    //(ABC , 版本)
    val ABC_versionRDD = sc.parallelize(ABC_version)

    //(ABC , type)
    val ABC_TypeRDD = sc.parallelize(ABC_Type)

    //(ABC , MN)
    val ABC_downRDD = sc.parallelize(ABC_down)

    //(MN , BCD)
    val MN_DownRDD = sc.parallelize(MN_down)

    //(MN , ABC)
    val MND_upRDD = sc.parallelize(MND_up)

    //(D , D.type)
    val D_TypeRdd = sc.parallelize(D_Type)

    //(D , db)
    val D_Type_dbRDD = sc.parallelize(D_Type_db)

    //(D , web)
    val D_Type_webRdd = sc.parallelize(D_Type_web)

    //(A , T , N)
    val timeRdd = sc.parallelize(ATNX)

    //得到(A , (down , typ , version))
    var fiRdd = ABC_downRDD
      .join(ABC_TypeRDD)
      .join(ABC_versionRDD)
      .map{case (an ,((down , typ) , version))
      =>
        (version+"_"+typ ,(an ,down,typ))
      }.filter(_._2._2.equals("transaction"))

    println("---------------------------------------------------------------------------------------------------------")
    fiRdd.collect().foreach(println(_))

    fiRdd.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        //myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        //myConf.set("hbase.zookeeper.property.clientPort","2181")
        //myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "BPMDEFINITION_NODE")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{
            var t = Array("web" , "db"  , "ftp" , "report" , "www" , "tv")
            var i = 0
            val p = new Put(Bytes.toBytes(item._1))
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("NODE_SERVICE_TYPE"), Bytes.toBytes(t((new util.Random).nextInt())))


            myTable.put(p)
          }

        }
        myTable.flushCommits()
      }
    }

  }
}
