#java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8449 -classpath `find lib/*| grep -v source | tr '\n' ':'` com.tencent.cubeli.orc.ORCWriterTest /data1/cubeli/orctest/tpch_2_17_0/dbgen/lineitem.tbl
java -classpath `find lib/*| grep -v source | tr '\n' ':'` com.tencent.cubeli.lucene.LuceneReaderTest