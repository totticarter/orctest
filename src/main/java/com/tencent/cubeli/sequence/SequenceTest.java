package com.tencent.cubeli.sequence;
import com.tencent.cubeli.common.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cubeli on 2017/7/31.
 */
public class SequenceTest {

    public static void main(String[] args) throws Exception{

        Configuration conf=new Configuration();
        FileSystem fs=FileSystem.get(conf);
        Path seqFile=new Path(Config.sequencePath);
        SequenceFile.Writer writer= SequenceFile.createWriter(fs,conf,seqFile,Text.class,Text.class);
        List<String> list = new ArrayList<>();


        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("Execute Hook.....");
            }
        }));


        for(int i = 0; i < Config.sequenceRocordSize; i++){

            list.add(new String(Config.testString));
            writer.append(new Text("key"+String.valueOf(i)),new Text(Config.testString));
            if(i%Config.syncNum == 0){

//                writer.hsync();
                Thread.sleep(100);
                System.out.println("hsynced " + i + " records");
            }
//            if(i == 100){
//
//                String test = null;
//                test.toString();
//            }
            Runtime.getRuntime().halt(0);
        }
        System.out.println("try to close....");
        IOUtils.closeStream(writer);

    }
}
