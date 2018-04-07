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



    public static List<List<String> > ABC_Type = new ArrayList<>();
    public static List<List<String>>  ABC_down = new ArrayList<>();
    public static List<List<String> > getABC_Type(){
        return ABC_Type;
    }
    public static List<List<String> > getABC_down(){
        return ABC_down;
    }


    public static void readABC(){
        try {
            Configuration conf = HBaseConfiguration.create();
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
                if(new String(rowKV.getFamily()).equals("INFO") && new String(rowKV.getQualifier()).equals("DEFTYPE") && new String(rowKV.getValue()).equals("flowchar")){
                    temp.add(new String(rowKV.getValue())); //类型
                    ABC_Type.add(new ArrayList<String>(temp));
                }
                if(new String(rowKV.getFamily()).equals("ITEM") && new String(rowKV.getQualifier()).substring(0,4).equals("DOWN")){
                    temp.add(new String(rowKV.getQualifier()).substring(5));  //子节点
                    ABC_down.add(new ArrayList<String>(temp));
                }
                temp.clear();
            }

        }
    }


}
