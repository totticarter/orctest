package com.tencent.cubeli.lucene;

import com.tencent.cubeli.common.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.solr.store.hdfs.HdfsDirectory;

import java.io.IOException;


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

//        NumericDocValues docVals1 = leafReader.getNumericDocValues("l_orderkey");
//        System.out.println(docVals1.get(0));
//        System.out.println(docVals1.get(2));

        long start = System.currentTimeMillis();

        NumericDocValues orderkeyDocValues = leafReader.getNumericDocValues("l_orderkey");
        NumericDocValues partkeyDocValues = leafReader.getNumericDocValues("l_partkey");
        NumericDocValues suppkeyDocValues = leafReader.getNumericDocValues("l_suppkey");
        NumericDocValues quantityDocValues = leafReader.getNumericDocValues("l_quantity");
        NumericDocValues extendedpriceDocValues = leafReader.getNumericDocValues("l_extendedprice");
        NumericDocValues discountDocValues = leafReader.getNumericDocValues("l_discount");
        NumericDocValues taxDocValues = leafReader.getNumericDocValues("l_tax");

        for(int i = 0; i < leafReader.maxDoc(); i++){

            long oneRowStart = System.nanoTime();
            orderkeyDocValues.get(i);
//            partkeyDocValues.get(i);
//            suppkeyDocValues.get(i);
//            quantityDocValues.get(i);
//            extendedpriceDocValues.get(i);
//            discountDocValues.get(i);
//            taxDocValues.get(i);

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

        }

        long end = System.currentTimeMillis();
        System.out.println("read " + leafReader.maxDoc() + " rows takes: " + (end-start) + " ms");

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
