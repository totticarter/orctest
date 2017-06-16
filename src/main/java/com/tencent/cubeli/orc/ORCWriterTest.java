package com.tencent.cubeli.orc;


import com.tencent.cubeli.common.Config;
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
import java.util.Iterator;
import java.util.Map;

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

        String lineitemDataFile = Config.lineitemDataFile;
        String orcfilePath = Config.orcFilePath;
        Configuration conf = new Configuration();
//        conf.addResource("/etc/hadoop/conf/hdfs-site.xml");
//        conf.addResource("/etc/hadoop/conf/core-site.xml");


        Iterator<Map.Entry<String, String>> confIter = conf.iterator();

        while (confIter.hasNext()) {
            Map.Entry<String, String> entry = confIter.next();
            String key = entry.getKey();
            String value = entry.getValue();

            System.out.println(key);
            System.out.println(value);
        }

//        FileSystem.getLocal(conf);
        CompressionKind compressionKind = getCompressionKind();
        Writer writer = OrcFile.createWriter(new Path(orcfilePath),
                OrcFile.writerOptions(conf)
                        .setSchema(schema)
                        .stripeSize(67108864)
                        .bufferSize(131072)
                        .blockSize(134217728)
                        .compress(compressionKind)
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
    }

    private static CompressionKind getCompressionKind() {
        CompressionKind compressionKind = null;
        if(Config.compress.equals("zlib")){

            compressionKind = CompressionKind.ZLIB;
        }else if(Config.compress.equals("snappy")){

            compressionKind = CompressionKind.SNAPPY;
        }else if(Config.compress.equals("lzo")){

            compressionKind = CompressionKind.LZO;
        }
        return compressionKind;
    }

}
