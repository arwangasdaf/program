package Main

import java.util

import Calculate.calculate
import Data.{Read, ReadATN, Te}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkContext

import collection.JavaConverters._

/**
  * Created by Administrator on 2017/4/11.
  */
object tt {
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

  def main(args: Array[String]): Unit = {
      val sc = new SparkContext();
      //从Hbase中读取SA至今的关系
     Read.readSA()
      SA = Read.getSA().asScala
      .map(son=>(son.get(0),son.get(1))).toArray

      //SA之间的关系 (S1 , A1)

      //从Hbase中读取ABC之间的关系
      //Read.readABC()
      print("PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP")
      ABC_version = Read.getABC_version.asScala
        .map(son=>(son.get(0),son.get(1))).toArray
      ABC_down = Read.getABC_down.asScala
        .map(son=>(son.get(0),son.get(1))).toArray
      ABC_Type = Read.getABC_Type.asScala
        .map(son=>(son.get(0),son.get(1))).toArray


     //从Hbase中读取MND之间的关系
     //Read.readMND()
     MN_down = Read.getMN_down.asScala
       .map(son=>(son.get(0),son.get(1))).toArray
     MND_up = Read.getMND_up.asScala
       .map(son=>(son.get(0),son.get(1))).toArray
     D_Type = Read.getD_Type.asScala
       .map(son=>(son.get(0),son.get(1))).toArray
     D_Type_db = Read.getD_Type_db.asScala
       .map(son=>(son.get(0),son.get(1))).toArray
     D_Type_web = Read.getD_Type_web.asScala
       .map(son=>(son.get(0),son.get(1))).toArray

    //读取ATN之间的关系
    ATNX = ReadATN.getATN.asScala
      .map(son => (son.get(0) , (son.get(1) , son.get(2)))).toArray

     //进行计算
     var cal = new calculate(
       SA ,
       ABC_version ,
       ABC_down ,
       ABC_Type ,
       MN_down ,
       MND_up ,
       D_Type,
       D_Type_db,
       D_Type_web,
       ATNX)

     cal.run(sc)
  }
}
