package com.tencent.cubeli.orc;

/**
 * Created by waixingren on 6/12/17.
 */
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import com.tencent.cubeli.common.Config;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;


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

//        conf.set("hive.io.file.readcolumn.ids", "1");


        int count = 0;
        long start = System.currentTimeMillis();

        Thread[] threads = new Thread[splits.length];
        int threadIdx = 0;
        for(InputSplit inputSplit : splits){

            RecordReader reader = in.getRecordReader(inputSplit, conf, Reporter.NULL);
            Object key = reader.createKey();
            Object value = reader.createValue();
            List<? extends StructField> fields = inspector.getAllStructFieldRefs();
            long offset = reader.getPos();
            while (reader.next(key, value)) {
                count++;

                for (int fieldIdx = 0; fieldIdx < Config.readFieldCount; fieldIdx++) {

                    Object object = inspector.getStructFieldData(value, fields.get(fieldIdx));
                    System.out.println(object.toString());
                }
            }


            //多线程测试
//            Thread t = new Thread(new SplitProcessor(inputSplit,inspector,conf));
//            t.start();
//            threads[threadIdx++] = t;
        }
        System.out.println("threads size is: " + threads.length);
//        for(Thread thread : threads){
//            thread.join();
//        }
        long end = System.currentTimeMillis();
        System.out.println( "takes: "+ (end-start) + " ms");

    }

    public void getValue(StructObjectInspector inspector, Object value, List<? extends StructField> fields){

            Object l_orderkey = inspector.getStructFieldData(value, fields.get(0));
            Object l_partkey = inspector.getStructFieldData(value, fields.get(1));
            Object l_suppkey = inspector.getStructFieldData(value, fields.get(2));
            Object l_linenumber = inspector.getStructFieldData(value, fields.get(3));
            Object l_quantity = inspector.getStructFieldData(value, fields.get(4));
            Object l_extendedprice = inspector.getStructFieldData(value, fields.get(5));
            Object l_discount = inspector.getStructFieldData(value, fields.get(6));
            Object l_tax = inspector.getStructFieldData(value, fields.get(7));
    }


    static class SplitProcessor implements Runnable{

        private InputSplit oneSplit = null;
        private StructObjectInspector inspector = null;
        private JobConf conf = null;
        public SplitProcessor(InputSplit oneSplit, StructObjectInspector inspector, JobConf conf){

            this.oneSplit = oneSplit;
            this.inspector = inspector;
            this.conf = conf;
        }

        @Override
        public void run() {

            try{

                System.out.println("start to read split: " + oneSplit.toString());
                long start = System.currentTimeMillis();
                InputFormat in = new OrcInputFormat();
                RecordReader reader = in.getRecordReader(oneSplit, conf, Reporter.NULL);
                Object key = reader.createKey();
                Object value = reader.createValue();
                List<? extends StructField> fields = inspector.getAllStructFieldRefs();
                long offset = reader.getPos();
                int count = 0;
                while (reader.next(key, value)) {
                    count++;

                    for (int fieldIdx = 0; fieldIdx < Config.readFieldCount; fieldIdx++) {

                        inspector.getStructFieldData(value, fields.get(fieldIdx));
                    }
                }
                System.out.println("count is: " + count);
                reader.close();
                long end = System.currentTimeMillis();
                System.out.println("one split takes: " + (end-start) + "ms");
            }catch (IOException io){

                io.printStackTrace();
            }
        }
    }

}
