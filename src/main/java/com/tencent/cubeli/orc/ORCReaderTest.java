package com.tencent.cubeli.orc;

/**
 * Created by waixingren on 6/12/17.
 */
import java.util.List;
import java.util.Properties;

import com.tencent.cubeli.common.Config;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * lxw的大数据田地 -- http://lxw1234.com
 * @author lxw.com
 *
 */
public class ORCReaderTest {

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf();
        Path testFilePath = new Path(Config.orcFilePath);
        Properties p = new Properties();
        OrcSerde serde = new OrcSerde();


        p.setProperty("columns", "l_orderkey,l_partkey,l_suppkey,l_linenumber," +
                "l_quantity,l_extendedprice,l_discount,l_tax,l_retrunflag" +
                ",l_linestatus,l_shipdate,l_commitdate,l_receiptdate," +
                "l_shipinstruct,l_shipmode,l_comment");
        p.setProperty("columns.types", "bigint:bigint:bigint:bigint:" +
                "double:double:double:double:string:" +
                "string:string:string:string:" +
                "string:string:string");
        serde.initialize(conf, p);
        StructObjectInspector inspector = (StructObjectInspector) serde.getObjectInspector();
        InputFormat in = new OrcInputFormat();
        FileInputFormat.setInputPaths(conf, testFilePath.toString());
        InputSplit[] splits = in.getSplits(conf, 1);
        System.out.println("splits.length==" + splits.length);

        conf.set("hive.io.file.readcolumn.ids", "1");
        RecordReader reader = in.getRecordReader(splits[0], conf, Reporter.NULL);
        Object key = reader.createKey();
        Object value = reader.createValue();
        List<? extends StructField> fields = inspector.getAllStructFieldRefs();
        long offset = reader.getPos();

        long start = System.currentTimeMillis();
        int count = 0;
        while(reader.next(key, value)) {
            count++;
            Object l_orderkey = inspector.getStructFieldData(value, fields.get(0));
            Object l_partkey = inspector.getStructFieldData(value, fields.get(1));
            Object l_suppkey = inspector.getStructFieldData(value, fields.get(2));
            Object l_linenumber = inspector.getStructFieldData(value, fields.get(3));
            Object l_quantity = inspector.getStructFieldData(value, fields.get(4));
            Object l_extendedprice = inspector.getStructFieldData(value, fields.get(5));
            Object l_discount = inspector.getStructFieldData(value, fields.get(6));
            Object l_tax = inspector.getStructFieldData(value, fields.get(7));
            System.out.println(l_orderkey+"|"+l_partkey+"|"+l_suppkey+"|"+l_linenumber+"|"+l_quantity+"|"+l_extendedprice+"|"+l_discount+"|"+l_tax);
            Thread.sleep(500);

        }
        long end = System.currentTimeMillis();
        System.out.println("read " + count + "rows takes: " + (end-start) + " ms");
        reader.close();

    }

}
