package src.Main

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import src.Data.Read

import collection.JavaConverters._
/**
  * Created by Administrator on 2017/4/21.
  */
object launch {
  var db : Array[(String,(String))] = _
  var web : Array[(String,(String))] = _
  var ftp: Array[(String,(String))] = _
  var map : Array[(String,(String))] = _
  var mail : Array[(String,(String))] = _
  var Internet : Array[(String,(String))] = _



  def main(args: Array[String]): Unit = {
   val sc = new SparkContext()
    /*val t = new Te()
    t.run(sc)*/
    //从Hbase中读取SA至今的关系

    Read.readMND()
    db = Read.getDb.asScala
      .map(son=>(son.get(0),son.get(1))).toArray


    var dbRDD = sc.parallelize(db , 100)  //web


    web = Read.getWeb.asScala
      .map(son=>(son.get(0),son.get(1))).toArray


    var webRDD = sc.parallelize(web , 100)

    ftp = Read.getFtp.asScala
      .map(son=>(son.get(0),son.get(1))).toArray


    var ftpRDD = sc.parallelize(ftp , 100)

    mail = Read.getMail.asScala
      .map(son=>(son.get(0),son.get(1))).toArray


    var mailRDD = sc.parallelize(mail , 100)

    map = Read.getMap.asScala
      .map(son=>(son.get(0),son.get(1))).toArray


    var mapRDD = sc.parallelize(map , 100)

    Internet = Read.getInternet.asScala
      .map(son=>(son.get(0),son.get(1))).toArray


    var InternetRDD = sc.parallelize(Internet , 100)


    //MBTRDD.collect().foreach(println(_))

    /**
      * 生成 AM 之间的关系
      */
    dbRDD.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        myConf.set("hbase.zookeeper.property.clientPort","2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "DEINFO")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{

            val p = new Put(Bytes.toBytes(item._1))

            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("TYPE"), Bytes.toBytes("db"))
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("G"), Bytes.toBytes("102"))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("E_TPCC"), Bytes.toBytes("4"))

            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }


    webRDD.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        myConf.set("hbase.zookeeper.property.clientPort","2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "DEINFO")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{

            val p = new Put(Bytes.toBytes(item._1))

            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("TYPE"), Bytes.toBytes("web"))
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("G"), Bytes.toBytes("103"))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("E_File"), Bytes.toBytes("2"))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("E_CPU"), Bytes.toBytes("2"))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("E_EJB"), Bytes.toBytes("2"))
            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }

    ftpRDD.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        myConf.set("hbase.zookeeper.property.clientPort","2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "DEINFO")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{

            val p = new Put(Bytes.toBytes(item._1))

            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("TYPE"), Bytes.toBytes("ftp"))
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("G"), Bytes.toBytes("104"))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("E_DATA"), Bytes.toBytes("3"))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("E_FILE"), Bytes.toBytes("2"))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("E_CPU"), Bytes.toBytes("6"))

            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }

    mapRDD.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        myConf.set("hbase.zookeeper.property.clientPort","2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "DEINFO")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{

            val p = new Put(Bytes.toBytes(item._1))

            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("TYPE"), Bytes.toBytes("map"))
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("G"), Bytes.toBytes("105"))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("E_DATA"), Bytes.toBytes("5"))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("E_FILE"), Bytes.toBytes("6"))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("E_CPU"), Bytes.toBytes("1"))

            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }

    mailRDD.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        myConf.set("hbase.zookeeper.property.clientPort","2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "DEINFO")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{

            val p = new Put(Bytes.toBytes(item._1))

            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("TYPE"), Bytes.toBytes("mail"))
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("G"), Bytes.toBytes("106"))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("E_DATA"), Bytes.toBytes("3"))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("E_FILE"), Bytes.toBytes("5"))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("E_CPU"), Bytes.toBytes("3"))

            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }
    InternetRDD.foreachPartition{
      triple => {
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3")
        myConf.set("hbase.zookeeper.property.clientPort","2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, "DEINFO")
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(5*1024*1024)
        triple.foreach{
          item=>{

            val p = new Put(Bytes.toBytes(item._1))

            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("TYPE"), Bytes.toBytes("Internet"))
            p.add(Bytes.toBytes("INFO"), Bytes.toBytes("G"), Bytes.toBytes("107"))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("E_DATA"), Bytes.toBytes("1"))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("E_FILE"), Bytes.toBytes("3"))
            p.add(Bytes.toBytes("ITEM"), Bytes.toBytes("E_CPU"), Bytes.toBytes("6"))

            myTable.put(p)
          }
        }
        myTable.flushCommits()
      }
    }
  }
}
