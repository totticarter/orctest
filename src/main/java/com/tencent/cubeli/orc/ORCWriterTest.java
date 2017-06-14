package com.tencent.cubeli.orc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class ORCWriterTest {

    public static TypeDescription schema = TypeDescription.createStruct()
            .addField(Lineitem.l_orderkey, TypeDescription.createLong())
            .addField(Lineitem.l_partkey, TypeDescription.createLong())
            .addField(Lineitem.l_suppkey, TypeDescription.createLong())
            .addField(Lineitem.l_linenumber, TypeDescription.createLong())
            .addField(Lineitem.l_quantity, TypeDescription.createDouble())
            .addField(Lineitem.l_extendedprice, TypeDescription.createDouble())
            .addField(Lineitem.l_discount, TypeDescription.createDouble())
            .addField(Lineitem.l_tax, TypeDescription.createDouble())
            .addField(Lineitem.l_retrunflag, TypeDescription.createString())
            .addField(Lineitem.l_linestatus, TypeDescription.createString())
            .addField(Lineitem.l_shipdate, TypeDescription.createString())
            .addField(Lineitem.l_commitdate, TypeDescription.createString())
            .addField(Lineitem.l_receiptdate, TypeDescription.createString())
            .addField(Lineitem.l_shipinstruct, TypeDescription.createString())
            .addField(Lineitem.l_shipmode, TypeDescription.createString())
            .addField(Lineitem.l_comment, TypeDescription.createString());
    public static void main(String[] args) throws Exception {

        String lineitemDataFile = args[0];
//        TypeDescription schema = TypeDescription.createStruct()
//                .addField(Lineitem.l_orderkey, TypeDescription.createLong())
//                .addField(Lineitem.l_partkey, TypeDescription.createLong())
//                .addField(Lineitem.l_suppkey, TypeDescription.createLong())
//                .addField(Lineitem.l_linenumber, TypeDescription.createLong())
//                .addField(Lineitem.l_quantity, TypeDescription.createDouble())
//                .addField(Lineitem.l_extendedprice, TypeDescription.createDouble())
//                .addField(Lineitem.l_discount, TypeDescription.createDouble())
//                .addField(Lineitem.l_tax, TypeDescription.createDouble())
//                .addField(Lineitem.l_retrunflag, TypeDescription.createString())
//                .addField(Lineitem.l_linestatus, TypeDescription.createString())
//                .addField(Lineitem.l_shipdate, TypeDescription.createString())
//                .addField(Lineitem.l_commitdate, TypeDescription.createString())
//                .addField(Lineitem.l_receiptdate, TypeDescription.createString())
//                .addField(Lineitem.l_shipinstruct, TypeDescription.createString())
//                .addField(Lineitem.l_shipmode, TypeDescription.createString())
//                .addField(Lineitem.l_commitdate, TypeDescription.createString());
        //输出ORC文件本地绝对路径
//        String lxw_orc1_file = "lineitem.orc";
        String orcfilePath = "hdfs://localhost:9000/testorc/orcfile";
        Configuration conf = new Configuration();
//        FileSystem.getLocal(conf);
        Writer writer = OrcFile.createWriter(new Path(orcfilePath),
                OrcFile.writerOptions(conf)
                        .setSchema(schema)
                        .stripeSize(67108864)
                        .bufferSize(131072)
                        .blockSize(134217728)
                        .compress(CompressionKind.ZLIB)
                        .version(OrcFile.Version.V_0_12));
        //要写入的内容

        BufferedReader reader = new BufferedReader(new FileReader(new File(lineitemDataFile)));
        String oneLine = null;

        VectorizedRowBatch batch = schema.createRowBatch();
        int rowCount = 0;
        while ((oneLine = reader.readLine()) != null) {

            rowCount++;

            if(rowCount % 100 == 0){
                System.out.println(rowCount);
            }
            String[] datas = oneLine.split("\\|");
            for (int i = 0; i < datas.length; i++) {
                String typeName = schema.getChildren().get(i).getCategory().toString();
                if(typeName.equals("LONG")){

                    Long val = Long.parseLong(datas[i]);
//                    ((LongColumnVector) batch.cols[i]).fill(val.longValue());
                    LongColumnVector longColumnVector = ((LongColumnVector) batch.cols[i]);
                    longColumnVector.vector[batch.size] = val;
                }else if(typeName.equals("DOUBLE")){

                    Double val = Double.parseDouble(datas[i]);
//                    ((DoubleColumnVector)batch.cols[i]).fill(val);
                    DoubleColumnVector doubleColumnVector = ((DoubleColumnVector)batch.cols[i]);
                    doubleColumnVector.vector[batch.size] = val;
                }else if(typeName.equals("STRING")){

                    String val = datas[i];
//                    ((BytesColumnVector)batch.cols[i]).fill(val.getBytes());
                    BytesColumnVector bytesColumnVector = ((BytesColumnVector)batch.cols[i]);
                    bytesColumnVector.vector[batch.size] = val.getBytes();
                }
                if (batch.size == batch.getMaxSize()-1) {
                    System.out.println("batch.size is: " + batch.size);
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            batch.size++;
        }
        System.out.println("batch size is: " + batch.size);
        writer.addRowBatch(batch);
        writer.close();
//        String[] contents = new String[]{"1,a,aa","2,b,bb","3,c,cc","4,d,dd"};
//
//        VectorizedRowBatch batch = schema.createRowBatch();
//        for(String content : contents) {
//            int rowCount = batch.size++;
//            String[] logs = content.split(",", -1);
//            for(int i=0; i<logs.length; i++) {
//                ((BytesColumnVector) batch.cols[i]).setVal(rowCount, logs[i].getBytes());
//                //batch full
//                if (batch.size == batch.getMaxSize()) {
//                    writer.addRowBatch(batch);
//                    batch.reset();
//                }
//            }
//        }
    }

}
