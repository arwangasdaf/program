package Data;

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



    public static List<List<String> > dbn = new ArrayList<>();
    public static List<List<String>>  webn = new ArrayList<>();
    public static List<List<String> > getDbn(){
        return dbn;
    }
    public static List<List<String> > getWebn(){

        return webn;
    }


    public static void readABC(){
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "master,servant1,servant2,servant3");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.defaults.for.version.skip", "true");
            String tablename = "ADN";
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
                if(new String(rowKV.getFamily()).substring(0,2).equals("DB")){
                    temp.add(new String(rowKV.getQualifier()));
                    temp.add(new String(rowKV.getValue()));// D
                    dbn.add(new ArrayList<String>(temp));
                    System.out.println("*****************************");
                }
                if(new String(rowKV.getFamily()).substring(0,2).equals("WE")){
                    temp.add(new String(rowKV.getQualifier()));  // D
                    temp.add(new String(rowKV.getValue()));
                    webn.add(new ArrayList<String>(temp));
                    System.out.println("_________________________________________________");
                }
                temp.clear();
            }

        }
    }


}
