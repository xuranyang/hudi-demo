package com.atguigu.hudi.test

import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.index.HoodieIndex
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object StructuredStreamingTest {
  case class Model(userid: Long, username: String, age: Int, partition: Int, ts: Long)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("test-app")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.shuffle.partitions", "12")
    //     .setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val df = sparkSession.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103:9092")
      .option("subscribe", "test2")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", "20000")
      .load()

    import sparkSession.implicits._
    val query = df.selectExpr("cast (value as string)").as[String]
      .map(item => {
        val jsonObj: JSONObject = JSON.parseObject(item)
        val userid = jsonObj.getString("userid").toLong;
        val username = jsonObj.getString("username")
        val age = jsonObj.getString("age").toInt
        val partition = jsonObj.getString("partition").toInt
        val ts = System.currentTimeMillis()
        new Model(userid, username, age, partition, ts)
      }).writeStream.foreachBatch { (batchDF: Dataset[Model], batchid: Long) =>
      batchDF.write.format("hudi")
        .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL) //选择表的类型 到底是MERGE_ON_READ 还是 COPY_ON_WRITE
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "userid") //设置主键
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "ts") //数据更新时间戳的
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "partition") //hudi分区列
        .option("hoodie.table.name", "kafak_test_tabel") //hudi表名
        .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://hadoop101:10000") //hiveserver2地址
        .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default") //设置hudi与hive同步的数据库
        .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "kafak_test_tabel") //设置hudi与hive同步的表名
        .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "partition") //hive表同步的分区列
        .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName) // 分区提取器 按/ 提取分区
        .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true") //设置数据集注册并同步到hive
        .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true") //设置当分区变更时，当前数据的分区目录是否变更
        .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name()) //设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
        .option("hoodie.insert.shuffle.parallelism", "12")
        .option("hoodie.upsert.shuffle.parallelism", "12")
        .mode(SaveMode.Append)
        .save("/ss/hudi/test-table")
    }.option("checkpointLocation", "/ss/checkpoint")
      //      .trigger(Trigger.ProcessingTime(5, TimeUnit.MINUTES))
      .start()
    query.awaitTermination()

  }
}