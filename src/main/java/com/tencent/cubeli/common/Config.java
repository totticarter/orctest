package com.tencent.cubeli.common;

import java.io.*;
import java.util.Properties;

/**
 * Created by cubeli on 2017/6/16.
 */
public class Config {

    public static Properties prop = null;
    public static String compress = null;
    public static String orcFilePath = null;
    public static String lineitemDataFile = null;
    public static String luceneIndexDir = null;
    public static int readFieldCount = 0;
    public static int luceneMergeSize = 0;

    static {
        prop = new Properties();
        try{
            InputStream in = new BufferedInputStream(new FileInputStream("conf/properties.properties"));
            prop.load(in);
            compress = prop.getProperty("compress", "ZIP");
            orcFilePath = prop.getProperty("orc.file.path");
            lineitemDataFile = prop.getProperty("lineitem.data.file");
            luceneIndexDir = prop.getProperty("lucene.index.dir");
            readFieldCount = Integer.parseInt(prop.getProperty("read.field.count"));
            luceneMergeSize = Integer.parseInt(prop.getProperty("lucene.merge.size"));

        }catch (FileNotFoundException f){

            f.printStackTrace();
        }catch (IOException io){
            io.printStackTrace();
        }
    }

}
