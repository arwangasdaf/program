package Data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkContext;

import java.io.IOException;
import java.util.*;
import Data.ReadATN;

/**
 * Created by Administrator on 2017/4/13.
 */
public class Read {

    /*
     *  readSA() 读取Hbase里的 SUBSYSTEM 表里的数据
     *  SA里的内容如下 [(S1 , A1),(S1 , A2),(S2 , A3)]
     */
    public static List<List<String> > SA = new ArrayList<>();
    public static List<List<String> > getSA(){
        return SA;
    }

    public static void readSA(){
        try {
            Configuration conf = HBaseConfiguration.create();
            String tablename = "";
            getAllSA(tablename , conf);
            readABC(conf);
            readMND(conf);
            ReadATN.readATN(conf);
            readwd(conf);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void getAllSA(String tableName , Configuration conf) throws Exception {
        HTable table = new HTable(conf, tableName);
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        // 输出结果
        for (Result result : results) {

            for (KeyValue rowKV : result.raw()) {
                List<String> temp = new ArrayList<>();
                String sNum = new String(rowKV.getRow());
                temp.add(sNum);
                //取得子系统里的流程链
                if(new String(rowKV.getFamily()).equals("ITEM")){
                    temp.add(new String(rowKV.getValue()));
                    SA.add(new ArrayList<String>(temp));
                }
                temp.clear();

               /* System.out.print("行名:" + new String(rowKV.getRow()) + " ");
                System.out.print("时间戳:" + rowKV.getTimestamp() + " ");
                System.out.print("列族名:" + new String(rowKV.getFamily()) + " ");
                System.out.print("列名:" + new String(rowKV.getQualifier()) + " ");
                System.out.println("值:" + new String(rowKV.getValue(),"UTF-8"));*/
            }

        }
    }

     /*
      *readAMBNC()  读取Hbase里的 "BPMDEFINITION" 表
      * 得到 ABC_version集合
      * 得到d
      * [(A1 , 10000 , flowchar) , (A2 , 100000 , operator) , (A3 , 19999 , transcition)]
      */
    public static List<List<String> > ABC_version = new ArrayList<>();  //(ABC , version , Type)
    public static List<List<String> > ABC_Type = new ArrayList<>();
    public static List<List<String>>  ABC_down = new ArrayList<>();
    public static List<List<String> > getABC_version(){
        return ABC_version;
    }
    public static List<List<String> > getABC_Type(){
        return ABC_Type;
    }
    public static List<List<String> > getABC_down(){
        return ABC_down;
    }


    public static void readABC(Configuration conf){
        try {
            String tablename = "BPMDEFINITION";
            getAllABC(tablename , conf);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void getAllABC(String tableName , Configuration conf) throws Exception {
        HTable table = new HTable(conf, tableName);
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        // 输出结果
        for (Result result : results) {

            for (KeyValue rowKV : result.raw()) {
                //(ABC_type , version)
                List<String> temp = new ArrayList<>();
                String sNum = new String(rowKV.getRow());
                temp.add(sNum);
                //取得版本号
                if(new String(rowKV.getFamily()).equals("INFO") && new String(rowKV.getQualifier()).equals("ACTDEFID")){
                    temp.add(new String(rowKV.getValue())); //版本号
                    ABC_version.add(new ArrayList<String>(temp));
                }
                if(new String(rowKV.getFamily()).equals("INFO") && new String(rowKV.getQualifier()).equals("DEFTYPE")){
                    temp.add(new String(rowKV.getValue())); //类型
                    ABC_Type.add(new ArrayList<String>(temp));
                }
                if(new String(rowKV.getFamily()).equals("ITEM") && new String(rowKV.getQualifier()).substring(0,4).equals("DOWN")){
                    temp.add(new String(rowKV.getQualifier()).substring(5));  //子节点
                    ABC_down.add(new ArrayList<String>(temp));
                }
/*
                System.out.print("行名:" + new String(rowKV.getRow()) + " ");
                System.out.print("时间戳:" + rowKV.getTimestamp() + " ");
                System.out.print("列族名:" + new String(rowKV.getFamily()) + " ");
                System.out.print("列名:" + new String(rowKV.getQualifier()) + " ");
                System.out.println("值:" + new String(rowKV.getValue(),"UTF-8"));*/
                temp.clear();
            }

        }
    }

    /*
     *MND 级别的处理
     */

    public static List<List<String> > MN_down = new ArrayList<>();
    public static List<List<String> > MND_up = new ArrayList<>();
    public static List<List<String> > D_Type = new ArrayList<>();
    public static List<List<String> > D_Type_db = new ArrayList<>();
    public static List<List<String> > D_Type_web = new ArrayList<>();
    public static List<List<String> > getMN_down(){
        return MN_down;
    }
    public static List<List<String> > getMND_up(){
        return MND_up;
    }
    public static List<List<String> > getD_Type(){
        return D_Type;
    }
    public static List<List<String> > getD_Type_db(){
        return D_Type_db;
    }
    public static List<List<String> > getD_Type_web(){
        return D_Type_web;
    }



    public static void readMND(Configuration conf){
        try {
            String tablename = "BPMDEFINITION_NODE";
            getAllMND(tablename , conf);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void getAllMND(String tableName , Configuration conf) throws Exception {
        HTable table = new HTable(conf, tableName);
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        // 输出结果
        for (Result result : results) {

            for (KeyValue rowKV : result.raw()) {
                List<String> temp = new ArrayList<>();
                //(ABC_type , version)
                String sNum = new String(rowKV.getRow());
                temp.add(sNum);
                //取得版本号
                if(new String(rowKV.getFamily()).equals("INFO") && new String(rowKV.getQualifier()).equals("DOWN_DEFKEY")){
                    temp.add(new String(rowKV.getValue()));
                    MN_down.add(new ArrayList<String>(temp));
                }
                if(new String(rowKV.getFamily()).equals("INFO") && new String(rowKV.getQualifier()).equals("UP_DEFKEY")){
                    temp.add(new String(rowKV.getValue()));
                    MND_up.add(new ArrayList<String>(temp));
                }
                if(new String(rowKV.getFamily()).equals("INFO") && new String(rowKV.getQualifier()).equals("NODE_SERVICE_TYPE")){
                    temp.add(new String(rowKV.getValue()));
                    D_Type.add(new ArrayList<String>(temp));
                    //找出Type为db的类型
                    if(new String(rowKV.getValue()).equals("db")){
                        D_Type_db.add(new ArrayList<String>(temp));
                    }
                    if(new String(rowKV.getValue()).equals("web")){
                        D_Type_web.add(new ArrayList<String>(temp));
                    }
                    //找出Type为web的类型
                }
                temp.clear();
              /*  System.out.print("行名:" + new String(rowKV.getRow()) + " ");
                System.out.print("时间戳:" + rowKV.getTimestamp() + " ");
                System.out.print("列族名:" + new String(rowKV.getFamily()) + " ");
                System.out.print("列名:" + new String(rowKV.getQualifier()) + " ");
                System.out.println("值:" + new String(rowKV.getValue(),"UTF-8"));*/
            }

        }
    }


     //web , db级别处理
    public static List<List<String> > db = new ArrayList<>();
    public static List<List<String> > webr = new ArrayList<>();
    public static List<List<String> > getDb(){
        return db;
    }
    public static List<List<String> > getWeb(){
        return webr;
    }



    public static void readwd(Configuration conf){
        try {
            String tablename = "D_DB_WEB";
            getwebdb(tablename , conf);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void getwebdb(String tableName , Configuration conf) throws Exception {
        HTable table = new HTable(conf, tableName);
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        // 输出结果
        for (Result result : results) {

            for (KeyValue rowKV : result.raw()) {
                List<String> temp = new ArrayList<>();
                //(ABC_type , version)
                String sNum = new String(rowKV.getRow());
                temp.add(sNum);
                //取得版本号
                if(new String(rowKV.getFamily()).equals("Table")){
                    temp.add(new String(rowKV.getQualifier()));
                    temp.add(new String(rowKV.getValue()));
                    db.add(new ArrayList<String>(temp));
                }
                if(new String(rowKV.getFamily()).equals("Page")){
                    temp.add(new String(rowKV.getQualifier()));
                    temp.add(new String(rowKV.getValue()));
                    webr.add(new ArrayList<String>(temp));
                }
                temp.clear();
                System.out.print("行名:" + new String(rowKV.getRow()) + " ");
                System.out.print("时间戳:" + rowKV.getTimestamp() + " ");
                System.out.print("列族名:" + new String(rowKV.getFamily()) + " ");
                System.out.print("列名:" + new String(rowKV.getQualifier()) + " ");
                System.out.println("值:" + new String(rowKV.getValue(),"UTF-8"));
            }
        }
    }
}
