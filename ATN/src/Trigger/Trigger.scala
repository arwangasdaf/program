package Trigger

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/4/16.
  */
class Trigger {
  var processname: String = _
  var runtime: Int = _
  var A : Array[(String)] = _
  var hour: Int = 8

  var burden: Int = _
  var dt: Int = 1
  def this(A : Array[(String)],runtime: Int, burden: Int) {
    this()
    this.A = A
    this.runtime = runtime
    this.burden = burden
  }
  def run(): ArrayBuffer[(String, (String, String))] = {
    if (0 == this.dt) {
      System.err.println("Usage: Time Error ===> Wrong value of dt")
      System.exit(7)
    }
    println("--------------------------------------A-----------------------")
    A.foreach(println(_))
    println("---------------------------------------------------------------")
    var localtime = 0
    var last = 0
    var cnt = 0
    var min: String = ""
    var eventuate = ArrayBuffer[(String, (String, String))]()



      var j = 1
      last = 0
      localtime = 0
      hour = 8
      cnt = 0
      while (localtime <= runtime) {
        print("^&^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
        var y = localtime % 60
        if (y < 10) {
          min = "0" + y
        } else {
          min = y.toString
        }
        var time = hour + ":" + min
        val cike = (j % 6) + 20
        val event = (A((new util.Random).nextInt(A.length)), (time, cike.toString))
        eventuate += event
        localtime = localtime + dt
        last = last + 1
        if (last == 60) {
          last = 0;
          cnt = cnt + 1
          hour = 8 + cnt
        }
        j = j + 1
      }

    return eventuate.map(x => (x._1, (x._2._1, x._2._2))) //(A , T , N)
  }
  println("===================================> constructed a trigger success <=====================================")
}