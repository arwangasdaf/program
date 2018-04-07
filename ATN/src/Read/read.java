package Read;

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
 * Created by Administrator on 2017/4/16.
 */
public class read {
    /*
     *  readSA() 读取Hbase里的 SUBSYSTEM 表里的数据
     *  SA里的内容如下 [(S1 , A1),(S1 , A2),(S2 , A3)]
     */
    public static List<List<String>> SA = new ArrayList<>();
    public static List<List<String> > getSA(){
        return SA;
    }

    public static void readSA(){
        try {
            Configuration conf = HBaseConfiguration.create();
            String tablename = "SUBSYSTEM";
            getAllSA(tablename , conf);
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
                System.out.print("行名:" + new String(rowKV.getRow()) + " ");
                System.out.print("时间戳:" + rowKV.getTimestamp() + " ");
                System.out.print("列族名:" + new String(rowKV.getFamily()) + " ");
                System.out.print("列名:" + new String(rowKV.getQualifier()) + " ");
                System.out.println("值:" + new String(rowKV.getValue(),"UTF-8"));
            }

        }
    }
}
