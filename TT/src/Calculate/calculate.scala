package Calculate

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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
  var db: Array[(String , (String , String))] = _
  var web: Array[(String , (String , String))] = _


  def this(SA : Array[(String,(String))] ,
  ABC_version : Array[(String , (String))] ,
  ABC_Type : Array[(String , (String))] ,
  ABC_down : Array[(String , (String))] ,
  MN_down : Array[(String , (String))] ,
  MND_up : Array[(String , (String))] ,
  D_Type : Array[(String , (String))] ,
  D_Type_db : Array[(String , (String))] ,
  D_Type_web : Array[(String , (String))] ,
           web: Array[(String , (String , String))],
           db: Array[(String , (String , String))] ,
  ATNX : Array[(String , (String , String))]
          )
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
    this.web = web
    this.db = db
  }

  def run(sc: SparkContext): RDD[String] ={
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

    //
    val webRdd = sc.parallelize(web)

    //
    val dbRdd = sc.parallelize(db)
    //dbRdd.collect().foreach(println(_))
    println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
    println("#################################################################")
    //webRdd.collect().foreach(println(_))

    //得到(A , (down , typ , version))
    val adtvRdd =
    ABC_downRDD
      .join(ABC_TypeRDD)
      .join(ABC_versionRDD)
      .map{case (an ,((down , typ) , version))
      =>
        (version+"_"+typ ,(an ,down))
      }

    //adtvRdd.collect().foreach(println(_))

    //得到(B , (down , version , Typ))
    var bdtvRdd =
    ABC_downRDD
      .join(ABC_TypeRDD)
      .join(ABC_versionRDD)
      .map{case (bn ,((down , typ) , version))
      =>
        (bn ,(typ ,version , down))
      }

    //bdtvRdd.collect().foreach(println(_))

    //得到(Cn , (down , version , Typ)
    var cdtvRdd =
    ABC_downRDD
      .join(ABC_TypeRDD)
      .join(ABC_versionRDD)
      .map{case (cn ,((down , typ) , version))
      =>
        (cn , (typ , version , down))
      }

    //(B , (A , type))
    var batRdd =
    adtvRdd.join(MN_DownRDD)
      .map{case (version_task , ((an , typ),bn)) => (bn , (an , typ))}

    /*batRdd.foreachPartition{
      triple => {
        //myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        //myConf.set("hbase.zookeeper.property.clientPort","2181")
        //myConf.set("hbase.defaults.for.version.skip", "true")
        val myConf = HBaseConfiguration.create()
        val myTable = new HTable(myConf, "BPMDEFINITION_NODE")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{
            val p = new Put(Bytes.toBytes(item._1))
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("DOWN_DEFKEY"), Bytes.toBytes(item._1+"_"+111))
            myTable.put(p)
          }

        }
        myTable.flushCommits()
      }
    }*/

    //batRdd.collect().foreach(println(_))

    //(version+"_"+down , (An , Bn)
    var vdabRdd = bdtvRdd
      .join(batRdd)
      .map{case (bn , ((down , version , typ1),(an , typ2))) => (version+"_"+down , (an , bn))}

    //vdabRdd.collect().foreach(println(_))

    //(Cn , (An , Bn)
    var cabRdd = vdabRdd.join(MN_DownRDD).map{case (version , ((an ,bn),cn)) => (cn , (an , bn))}
    //cabRdd.collect().foreach(println(_))

    //(version+"_"+down , (An , Bn , Cn)
    var vabcRdd = cabRdd.join(cdtvRdd).map{case (cn , ((an ,bn) , (down , version , typ))) => (version+"_"+down , (an , bn , cn))}
    vabcRdd.join(dbRdd).map{case (version , ((a , b , c) , (table , caozuo))) => (a,(table , caozuo))}.join(timeRdd).collect().foreach(println(_))
    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")

    //vabcRdd.join(dbRdd).collect().foreach(println(_))

    //vabcRdd.collect().foreach(println(_))
    //(An , Bn , Cn , typ)

    //D_TypeRdd.collect().foreach(println(_))
    var abctRdd = vabcRdd.join(D_TypeRdd).map{case (version , ((an , bn , cn),typ)) => (an , bn , cn , typ)}

    var klklRdd = vabcRdd.join(D_TypeRdd).map{case (version , ((an , bn , cn),typ)) => (an , bn , cn , typ)}
    //abctRdd.collect().foreach{case (a , b , c , d) => println((a , b , c ,d))}

    // (wjd,hbasedmsc1,hbasedmsc,web)(hbasedmsc3,hbasedmsc1,hbasedmsc,web)(hbasedmsc2,hbasedmsc1,hbasedmsc,web)
    // (wjd,hbasedmsc1,hbasedmsc,web)(hbasedmsc3,hbasedmsc1,hbasedmsc,web)(hbasedmsc2,hbasedmsc1,hbasedmsc,web)
    // (wjd,hbasedmsc1,hbasedmsc,db)(hbasedmsc3,hbasedmsc1,hbasedmsc,db)(hbasedmsc2,hbasedmsc1,hbasedmsc,db)
    // (wjd,hbasedmsc1,hbasedmsc,db)(hbasedmsc3,hbasedmsc1,hbasedmsc,db)(hbasedmsc2,hbasedmsc1,hbasedmsc,db)


    //(An , typ , 1)
    var atRdd = abctRdd.map{case (a , b , c ,d) => ((a , d),1)}
    //(An , typ , n)
    var atnRdd = atRdd.reduceByKey(_+_)
    var adnRdd = atnRdd.map{case ((a , d) , n) => (a,(d , n))}

    //atnRdd.collect().foreach{case ((a,d),n) => println(a , d , n)}


    //(a , s)
    var ASRDD = SARDD.map{case (s , a) => (a , s)}

    //
    var ASDNRDD = ASRDD.join(adnRdd).map{case (a ,(s , (d , n))) => (a , (s , d , n))}
    //
    var fatnRdd = timeRdd.join(ASDNRDD).map{case (a , ((t , n1) , (s ,d ,n2))) => (s , a , t , d , (n1).toInt * n2 )}


    //fatnRdd.collect().foreach{case (s , a , t , d , n) => print(s , a , t , d , n)}
    println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")



    //模拟Db和web集合
    /*vabcRdd.foreachPartition{
      triple => {
        //myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        //myConf.set("hbase.zookeeper.property.clientPort","2181")
        //myConf.set("hbase.defaults.for.version.skip", "true")
        val myConf = HBaseConfiguration.create()
        val myTable = new HTable(myConf, "D_DB_WEB")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{
            val p = new Put(Bytes.toBytes(item._1))
            p.add(Bytes.toBytes("Table"),Bytes.toBytes("T1"),Bytes.toBytes("Insert"))
            p.add(Bytes.toBytes("Table"),Bytes.toBytes("T2"),Bytes.toBytes("Delete"))
            p.add(Bytes.toBytes("Table"),Bytes.toBytes("T3"),Bytes.toBytes("Update"))
            p.add(Bytes.toBytes("Table"),Bytes.toBytes("T4"),Bytes.toBytes("Query"))

            p.add(Bytes.toBytes("Page"),Bytes.toBytes("Page_1"),Bytes.toBytes("Read"))
            p.add(Bytes.toBytes("Page"),Bytes.toBytes("page_2"),Bytes.toBytes("Write"))
            p.add(Bytes.toBytes("Page"),Bytes.toBytes("page_3"),Bytes.toBytes("Read"))
            p.add(Bytes.toBytes("Page"),Bytes.toBytes("page_4"),Bytes.toBytes("Write"))

            myTable.put(p);
          }
        }
        myTable.flushCommits()
      }
    }*/
  }
}
