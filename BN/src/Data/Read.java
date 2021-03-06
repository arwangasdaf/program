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



    public static List<List<String> > MN_down = new ArrayList<>();
    public static List<List<String> > MND_up = new ArrayList<>();
    public static List<List<String> > getMN_down(){
        return MN_down;
    }
    public static List<List<String> > getMND_up(){
        return MND_up;
    }


    public static void readMND(){
        try {
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
                List<String> temp = new ArrayList<>();
                //(ABC_type , version)
                String sNum = new String(rowKV.getRow());
                temp.add(sNum);
                //得到所有含有DOWN_DEFKEY列的节点名称
                if(new String(rowKV.getFamily()).equals("INFO")
                        && new String(rowKV.getQualifier()).equals("DOWN_DEFKEY")){
                    temp.add(new String(rowKV.getValue()));
                    System.out.println(new String(rowKV.getValue()));
                    MN_down.add(new ArrayList<String>(temp));
                }
                //得到流程图节点,因为流程图节点会绑定操作图
                if(new String(rowKV.getFamily()).equals("INFO")
                        && new String(rowKV.getQualifier()).equals("NODETYPE")
                        && new String(rowKV.getValue()).equals("flowchar")){
                    System.out.println(new String(rowKV.getValue()));
                    temp.add(new String(rowKV.getValue()));
                    MND_up.add(new ArrayList<String>(temp));
                }
                temp.clear();
            }

        }
    }


}
