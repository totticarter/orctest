package com.tencent.cubeli.orc;

/**
 * Created by waixingren on 6/12/17.
 */
import java.util.List;
import java.util.Properties;

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
        Path testFilePath = new Path("hdfs://localhost:9000/testorc/orcfile");
        Properties p = new Properties();
        OrcSerde serde = new OrcSerde();

//        public static String l_orderkey = "l_orderkey";
//        public static String l_partkey = "l_partkey";
//        public static String l_suppkey = "l_suppkey";
//        public static String l_linenumber = "l_linenumber";
//        public static String l_quantity = "l_quantity";
//
//        public static String l_extendedprice = "l_extendedprice";
//        public static String l_discount = "l_discount";
//        public static String l_tax = "l_tax";
//
//        public static String l_retrunflag = "l_retrunflag";
//        public static String l_linestatus = "l_linestatus";
//        public static String l_shipdate = "l_shipdate";
//        public static String l_commitdate = "l_commitdate";
//        public static String l_receiptdate = "l_receiptdate";
//
//        public static String l_shipinstruct = "l_shipinstruct";
//        public static String l_shipmode = "l_shipmode";
//        public static String l_comment = "l_comment";

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
        while(reader.next(key, value)) {
            Object url = inspector.getStructFieldData(value, fields.get(0));
            Object word = inspector.getStructFieldData(value, fields.get(1));
            Object freq = inspector.getStructFieldData(value, fields.get(2));
            Object weight = inspector.getStructFieldData(value, fields.get(3));
            offset = reader.getPos();
            System.out.println(url + "|" + word + "|" + freq + "|" + weight);
        }
        reader.close();

    }

}
