package com.tencent.cubeli.orc2;


import com.facebook.presto.hive.orc.HdfsOrcDataSource;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.memory.AggregatedMemoryContext;
import com.facebook.presto.orc.metadata.OrcMetadataReader;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableMap;
import com.tencent.cubeli.common.Config;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.joda.time.DateTimeZone;

import java.io.EOFException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.concurrent.*;

/**
 * Created by waixingren on 8/13/17.
 */
public class OrcFilePerf {

        private static final String HADOOP_HOME = System.getenv("HADOOP_HOME");
        /**
         * @param
         */
        public OrcFilePerf() {
            super();
        }
        /**
         *     typeReading              * @param typeReading
         * @throws Exception
         */
        public void reading(String typeReading) throws Exception {
            if (!"byte".equalsIgnoreCase(typeReading) && !"presto".equalsIgnoreCase(typeReading)) {
                throw new Exception("error typeReading");
            }
            try {
                Configuration config = new Configuration();
//                config.addResource(new Path(HADOOP_HOME, "etc/hadoop/core-site.xml"));
//                config.addResource(new Path(HADOOP_HOME, "etc/hadoop/hdfs-site.xml"));
                Configuration conf=new Configuration();
                FileSystem fs = FileSystem.get(config);
                ArrayList<Future<ThreadRunInfo>> futureList = new ArrayList<Future<ThreadRunInfo>>();
                for (int i = 1; i <= 100; i++) {
                    futureList.clear();
                    this.reading(fs, futureList, typeReading, i);
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (URISyntaxException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (ExecutionException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        /**
         *           ,      * @param fs
         * @param futureList
         * @param typeReading
         * @param loopIdx
         * @throws Exception
         */
        private void reading(FileSystem fs, ArrayList<Future<ThreadRunInfo>> futureList,
                             String typeReading, int loopIdx) throws Exception {
            ExecutorService executorService = Executors.newFixedThreadPool(4);
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/tmp"), true);;
            long st = System.currentTimeMillis();
            while (files.hasNext()) {
                LocatedFileStatus fileStatus = files.next();
                if (fileStatus.getLen() > 0) {
                    Callable<ThreadRunInfo> task = null;
                    if ("byte".equalsIgnoreCase(typeReading)) {
                        task = this.byteReading(fs, fileStatus);
                    } else if ("presto".equalsIgnoreCase(typeReading)) {
                        task = this.prestoReading(fs, fileStatus);
                    }
                    if (null != task) {
                        futureList.add(executorService.submit(task));
                    } }
            }
            executorService.shutdown();
            executorService.awaitTermination(30, TimeUnit.MINUTES);
            long et = System.currentTimeMillis();
            long totalTime = 0;
            long totalSize = 0;
            for (Future<ThreadRunInfo> f : futureList) {
                totalTime += f.get().getTime();
                totalSize += f.get().getSize();
            }
            double sizeMB = totalSize / 1024.0 / 1024;
            double threadTime = totalTime / 1000.0;
            double wallTime = (et - st) / 1000.0;
            String s = "loop[%d]: total read %.2f MB, use %.2f seconds, speed: %.2f MB/s, total thread time %.2f seconds";
            System.out.println(String.format(s, loopIdx, sizeMB, wallTime, sizeMB / wallTime, threadTime));
        }
        /**
         *
         * @param fs
         * @param fileStatus
         * @return
         * @throws IOException */
        private Callable<ThreadRunInfo> byteReading(FileSystem fs, LocatedFileStatus fileStatus) throws IOException{
            Callable<ThreadRunInfo> task = () -> {
                try (FSDataInputStream inputStream = fs.open(fileStatus.getPath())) {
                    return readContent(inputStream);
                }
            };
            return task;
        }
        /**
         *   Presto   OrcReader     * @param fs
         * @param fileStatus
         * @return
         * @throws IOException
         */
        private Callable<ThreadRunInfo> prestoReading(FileSystem fs, LocatedFileStatus
                fileStatus) throws IOException {
            Callable<ThreadRunInfo> task = () -> {
                try (FSDataInputStream inputStream = fs.open(fileStatus.getPath())) {
                    HdfsOrcDataSource dataSource = new HdfsOrcDataSource(fileStatus.getPath().getName(),
                            fileStatus.getLen(),
                            new DataSize(1, DataSize.Unit.MEGABYTE),
                            new DataSize(8, DataSize.Unit.MEGABYTE),
                            new DataSize(8, DataSize.Unit.MEGABYTE), inputStream);
                    OrcReader orcReader = new OrcReader(dataSource,
                            new OrcMetadataReader(),
                            new DataSize(1, DataSize.Unit.MEGABYTE),
                            new DataSize(8, DataSize.Unit.MEGABYTE));
                    OrcRecordReader orcRecordReader = orcReader.createRecordReader(
                            ImmutableMap.of(13, VarcharType.VARCHAR, 23, VarcharType.VARCHAR
                                    OrcPredicate.TRUE, DateTimeZone.UTC, new AggregatedMemoryContext()));
                    long size = 0;
                    Block block = null;
                    long st = System.currentTimeMillis();
                    while(true) {
                        int batch = orcRecordReader.nextBatch();
                        if (batch <= 0) {
                            break; }
                        block = orcRecordReader.readBlock(VarcharType.VARCHAR, 13);
                        size += block.getSizeInBytes();
                        block = orcRecordReader.readBlock(VarcharType.VARCHAR, 23);
                        size += block.getSizeInBytes();
                    }
                    long et = System.currentTimeMillis();
                    return new ThreadRunInfo(et - st, size);
                }
            };
            return task;
        }
        private ThreadRunInfo readContent(FSDataInputStream inputStream) throws IOException {
            byte[] chunk = new byte[Config.chunkSize];
            long size = 0;
            long st = System.currentTimeMillis();
            while (true) {
                try {
                    inputStream.readFully(size, chunk);
                } catch (EOFException e) {
                    break;
                } finally {
                    size += chunk.length;
                } }
            long et = System.currentTimeMillis();
            return new ThreadRunInfo(et - st, size);
        }
    }

}
