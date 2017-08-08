package com.tencent.cubeli.sequence;

import com.tencent.cubeli.common.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

/**
 * Created by cubeli on 2017/8/1.
 */
public class ReadTest {

    public static void main(String[] args) throws Exception {

        Configuration conf=new Configuration();
        FileSystem fs=FileSystem.get(conf);
        Path seqFile=new Path(Config.sequencePath);
        SequenceFile.Reader reader=new SequenceFile.Reader(fs,seqFile,conf);

        Text key=new Text();
        Text value=new Text();
        while(reader.next(key,value)){
            System.out.println(key.toString() + value.toString());
        }
        IOUtils.closeStream(reader);
    }

}
