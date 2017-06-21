package com.tencent.cubeli.lucene;

import com.tencent.cubeli.common.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.*;
import org.apache.solr.store.hdfs.HdfsDirectory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by waixingren on 6/14/17.
 */
public class LuceneReaderTest {

    static double oneBatchTime = 0.0;
    public static void main(String[] args) throws IOException {


        Path path=new Path(Config.luceneIndexDir);
        Configuration conf=new Configuration();
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        HdfsDirectory directory=new HdfsDirectory(path, conf);

        DirectoryReader topReader = DirectoryReader.open(directory);
        LeafReader leafReader = topReader.leaves().get(0).reader();
        long start = System.currentTimeMillis();


//        ("columns", "l_orderkey,l_partkey,l_suppkey,l_linenumber," +
//                "l_quantity,l_extendedprice,l_discount,l_tax,l_retrunflag" +
//                ",l_linestatus,l_shipdate,l_commitdate,l_receiptdate," +
//                "l_shipinstruct,l_shipmode,l_comment");

        List<NumericDocValues> numericDocValues = new ArrayList<>();
        NumericDocValues orderkeyDocValues = leafReader.getNumericDocValues("l_orderkey");
        numericDocValues.add(orderkeyDocValues);
        NumericDocValues partkeyDocValues = leafReader.getNumericDocValues("l_partkey");
        numericDocValues.add(partkeyDocValues);
        NumericDocValues suppkeyDocValues = leafReader.getNumericDocValues("l_suppkey");
        numericDocValues.add(suppkeyDocValues);
        NumericDocValues linenumberDocValues = leafReader.getNumericDocValues("l_linenumber");
        numericDocValues.add(linenumberDocValues);
        NumericDocValues quantityDocValues = leafReader.getNumericDocValues("l_quantity");
        numericDocValues.add(quantityDocValues);
        NumericDocValues extendedpriceDocValues = leafReader.getNumericDocValues("l_extendedprice");
        numericDocValues.add(extendedpriceDocValues);
        NumericDocValues discountDocValues = leafReader.getNumericDocValues("l_discount");
        numericDocValues.add(discountDocValues);
        NumericDocValues taxDocValues = leafReader.getNumericDocValues("l_tax");
        numericDocValues.add(taxDocValues);


        List<SortedDocValues> binaryDocValues = new ArrayList<>();
        SortedDocValues retrunflagDocValues = leafReader.getSortedDocValues("l_retrunflag");
        binaryDocValues.add(retrunflagDocValues);
        SortedDocValues linestatusDocValues = leafReader.getSortedDocValues("l_linestatus");
        binaryDocValues.add(linestatusDocValues);
        SortedDocValues shipdateDocValues = leafReader.getSortedDocValues("l_shipdate");
        binaryDocValues.add(shipdateDocValues);
        SortedDocValues commitdateDocValues = leafReader.getSortedDocValues("l_commitdate");
        binaryDocValues.add(commitdateDocValues);
        SortedDocValues receiptdateDocValues = leafReader.getSortedDocValues("l_receiptdate");
        binaryDocValues.add(receiptdateDocValues);
        SortedDocValues shipinstructgDocValues = leafReader.getSortedDocValues("l_shipinstruct");
        binaryDocValues.add(shipinstructgDocValues);
        SortedDocValues shipmodeDocValues = leafReader.getSortedDocValues("l_shipmode");
        binaryDocValues.add(shipmodeDocValues);
        SortedDocValues commentDocValues = leafReader.getSortedDocValues("l_comment");
        binaryDocValues.add(commentDocValues);


        int count = 0;
        for(int i = 0; i < leafReader.maxDoc(); i++){

            count++;
            if(count % 10000000 == 0){
                System.out.println("have read " + count + " rows");
            }
//            long oneRowStart = System.nanoTime();
            for(int fieldIdx = 0; fieldIdx< Config.readFieldCount; fieldIdx++){

                if(fieldIdx<8){

                    numericDocValues.get(fieldIdx).get(i);
                }else{

                    SortedDocValues sortedDocValues = binaryDocValues.get(fieldIdx-8);
                    sortedDocValues.get(i);
                }
            }
            /*
            orderkeyDocValues.get(i);
            partkeyDocValues.get(i);
            suppkeyDocValues.get(i);
            quantityDocValues.get(i);
            extendedpriceDocValues.get(i);
            discountDocValues.get(i);
            taxDocValues.get(i);
            */

            /*
            long oneRowEnd = System.nanoTime();
            double oneRowTime = ((oneRowEnd-oneRowStart)/1000);
            if(oneRowTime > 0){

                System.out.println("oneRowTime is: " + oneRowTime + ", row count is: " + i);
            }
            oneBatchTime += oneRowTime;
            if(i % 1000 == 0){

                System.out.println("1000 row takes: " + oneBatchTime + "us");
                oneBatchTime = 0;
            }
            */

        }

        long end = System.currentTimeMillis();
        System.out.println("read " + count + " rows takes: " + (end-start) + " ms");

//        BinaryDocValues docVals2 = leafReader.getBinaryDocValues(BINARY_FIELD);
//        BytesRef bytesRef = docVals2.get(0);
//        System.out.println(bytesRef.utf8ToString());
//
//        SortedDocValues docVals3 = leafReader.getSortedDocValues(SORTED_FIELD);
//        String ordInfo = "", values = "";
//        for (int i = 0; i < leafReader.maxDoc(); ++i) {
//            ordInfo += docVals3.getOrd(i) + ":";
//            bytesRef = docVals3.get(i);
//            values += bytesRef.utf8ToString() + ":";
//        }
//        //2:1:0:3:-1
//        System.out.println(ordInfo);
//        //lucene:facet:abacus:search::
//        System.out.println(values);
//
//
//        SortedSetDocValues docVals = leafReader.getSortedSetDocValues(SORTEDSET_FIELD);
//        String info = "";
//        for (int i = 0; i < leafReader.maxDoc(); ++i) {
//            docVals.setDocument(i);
//            long ord;
//            info += "Doc " + i;
//            while ((ord = docVals.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
//                info += ", " + ord + "/";
//                bytesRef = docVals.lookupOrd(ord);
//                info += bytesRef.utf8ToString();
//            }
//            info += ";";
//        }
//
//        //从打印可以看出，每一个value都是在全局进行的排序，每个set中记录了每个value的全局序号
//        //Doc 0, 2/lucene, 3/search;Doc 1, 3/search;Doc 2, 0/abacus, 1/facet, 3/search;Doc 3;Doc 4;
//        System.out.println(info);
    }
}
