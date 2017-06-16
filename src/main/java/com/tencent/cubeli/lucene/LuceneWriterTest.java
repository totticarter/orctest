package com.tencent.cubeli.lucene;

/**
 * Created by waixingren on 6/14/17.
 */

import com.tencent.cubeli.orc.ORCWriterTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.store.hdfs.HdfsDirectory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class LuceneWriterTest {
    static final String NUMERIC_FIELD = "numeric";
    static final String BINARY_FIELD = "binary";
    static final String SORTED_FIELD = "sorted";
    static final String SORTEDSET_FIELD = "sortedset";

    static long[] numericVals = new long[] {12, 13, 0, 100, 19,12,19,0,13,100,100,19,20,18,12};
    static String[] binary = new String[] {"lucene", "doc", "value", "test", "example"};
    static String[] sortedVals = new String[] {"lucene", "facet", "abacus", "search", null};//abacus:0,facet:1,lucene:2,search:3
    static String[][] sortedSetVals = new String[][] {{"lucene", "search"}, {"search"}, {"facet", "abacus", "search"}, {}, {}};//abacus:0,facet:1,lucene:2,search:3

    static IndexReader topReader;
    static LeafReader leafReader;


    public static void main(String[] args) throws IOException {

	  writeIndex(args[0]);

    }

    public static void writeIndex(String sourceFile) throws IOException{

        Path path=new Path("hdfs://hdfsCluster/tmp/lucenedata");
        Configuration conf=new Configuration();
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        HdfsDirectory directory=new HdfsDirectory(path, conf);
        IndexWriterConfig config = new IndexWriterConfig(new  StandardAnalyzer());
        IndexWriter writer = new IndexWriter(directory, config);

        String lineitemDataFile = sourceFile;
        BufferedReader reader = new BufferedReader(new FileReader(new File(lineitemDataFile)));
        String oneLine = null;

        int rowCount = 0;
        while ((oneLine = reader.readLine()) != null) {

            if(rowCount++ % 100 == 0){

                System.out.println(rowCount);
            }
            Document doc = new Document();
            String[] datas = oneLine.split("\\|");
            for (int i = 0; i < datas.length; i++) {

                String typeName = ORCWriterTest.schema.getChildren().get(i).getCategory().toString();
                String fieldName = ORCWriterTest.schema.getFieldNames().get(i);
                if(typeName.equals("LONG")){

                    long val = Long.parseLong(datas[i]);
                    doc.add(new NumericDocValuesField(fieldName, val));
                }else if(typeName.equals("DOUBLE")){

                    double val = Double.parseDouble(datas[i]);
                    doc.add(new DoubleDocValuesField(fieldName, val));
                }else if(typeName.equals("STRING")){

                    doc.add(new SortedDocValuesField(fieldName, new BytesRef(datas[i])));
                }
            }
            writer.addDocument(doc);
        }

        writer.forceMerge(1);
        writer.commit();
        writer.close();
    }
}

