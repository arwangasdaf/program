package src.Data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/4/21.
 */
public class Read {



    public static List<List<String> > ftp = new ArrayList<>();
    public static List<List<String>> db = new ArrayList<>();
    public static List<List<String>> web = new ArrayList<>();

    public static List<List<String> > getFtp(){
        return ftp;
    }
    public static List<List<String> > getDb(){return db;}
    public static List<List<String> > getWeb(){return web;}

    public static List<List<String> > map = new ArrayList<>();
    public static List<List<String>> mail = new ArrayList<>();
    public static List<List<String>> Internet = new ArrayList<>();

    public static List<List<String> > getMap(){
        return ftp;
    }
    public static List<List<String> > getMail(){return db;}
    public static List<List<String> > getInternet(){return web;}

    public static void readMND(){
        try{
            Configuration conf = HBaseConfiguration.create();
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
                System.out.println("**************************************************************************************");
                List<String> temp = new ArrayList<>();
                //(ABC_type , version)
                String sNum = new String(rowKV.getRow());
                temp.add(sNum);
                //var a = Array("db" , "web" , "ftp" , "map" , "mail" , "Internet")
                if(new String(rowKV.getFamily()).equals("INFO") && new String(rowKV.getQualifier()).equals("NODE_SERVICE_TYPE") && new String(rowKV.getValue()).equals("db")){
                    temp.add(new String(rowKV.getValue()));
                    db.add(new ArrayList<String>(temp));
                }
                if(new String(rowKV.getFamily()).equals("INFO") && new String(rowKV.getQualifier()).equals("NODE_SERVICE_TYPE") && new String(rowKV.getValue()).equals("web")){
                    temp.add(new String(rowKV.getValue()));
                    web.add(new ArrayList<String>(temp));
                }
                if(new String(rowKV.getFamily()).equals("INFO") && new String(rowKV.getQualifier()).equals("NODE_SERVICE_TYPE") && new String(rowKV.getValue()).equals("ftp")){
                    temp.add(new String(rowKV.getValue()));
                    ftp.add(new ArrayList<String>(temp));
                }
                if(new String(rowKV.getFamily()).equals("INFO") && new String(rowKV.getQualifier()).equals("NODE_SERVICE_TYPE") && new String(rowKV.getValue()).equals("map")){
                    temp.add(new String(rowKV.getValue()));
                    map.add(new ArrayList<String>(temp));
                }
                if(new String(rowKV.getFamily()).equals("INFO") && new String(rowKV.getQualifier()).equals("NODE_SERVICE_TYPE") && new String(rowKV.getValue()).equals("mail")){
                    temp.add(new String(rowKV.getValue()));
                    mail.add(new ArrayList<String>(temp));
                }if(new String(rowKV.getFamily()).equals("INFO") && new String(rowKV.getQualifier()).equals("NODE_SERVICE_TYPE") && new String(rowKV.getValue()).equals("Internet")){
                    temp.add(new String(rowKV.getValue()));
                    Internet.add(new ArrayList<String>(temp));
                }


                temp.clear();
            }

        }
    }
}
