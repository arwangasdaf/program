package Main

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by Administrator on 2017/4/11.
  */
object launch {
  def main(args: Array[String]) {

    val sc = new SparkContext();

    val sqlContext = new SQLContext(sc);
    val df = sqlContext.read.json("/newdata.json")
    df.show()
  }
}