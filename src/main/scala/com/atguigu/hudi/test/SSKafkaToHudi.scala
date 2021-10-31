package com.atguigu.hudi.test

import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.hive.{MultiPartKeysValueExtractor, NonPartitionedExtractor}
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}


object SSKafkaToHudi {

  case class Model(op: String, database: String, table: String, afterData: String, beforeData: String, ts_ms: Long)

  case class Student(id: Int, name: String, age: Int, email: String, qq: String, ts: Long)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("test-app")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.shuffle.partitions", "12")
      .setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val df = sparkSession.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103:9092")
      .option("subscribe", "test-cdc")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", "20000")
      .load()
    import sparkSession.implicits._
    val query = df.selectExpr("cast (value as string)").as[String]
      .map(item => {
        val jsonObj: JSONObject = JSON.parseObject(item)
        val op = jsonObj.getString("op")
        val database = jsonObj.getString("daatabase")
        val table = jsonObj.getString("table")
        val afterData = if (jsonObj.containsKey("afterData")) jsonObj.getString("afterData") else ""
        val beforeData = if (jsonObj.containsKey("beforeData")) jsonObj.getString("beforeData") else ""
        val ts_ms = jsonObj.getString("ts_ms").toLong
        Model(op, database, table, afterData, beforeData, ts_ms)
      }).writeStream.foreachBatch { (batchDF: Dataset[Model], batchid: Long) =>
      batchDF.cache() //缓存数据
      val upsertData = batchDF.filter("op =='CREATE' or op =='UPDATE'") //新增数据 和修改数据
      val deleteData = batchDF.filter("op =='DELETE'") //删除数据
      upsertHudi(upsertData, sparkSession)
      deleteHudi(deleteData, sparkSession);
      batchDF.unpersist().show() //释放缓存
    }.option("checkpointLocation", "/ss/checkpoint")
      .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .start()
    query.awaitTermination()
  }

  /**
   * 新增和修改数据 方法
   *
   * @param batchDF
   * @param sparkSession
   */
  def upsertHudi(batchDF: Dataset[Model], sparkSession: SparkSession) = {
    //解析afterData里的json数据
    import sparkSession.implicits._
    val result = batchDF.mapPartitions(partitions => {
      partitions.map(item => {
        val aflterData = item.afterData
        val afterJson = JSON.parseObject(aflterData)
        val qq = afterJson.getString("qq")
        val name = afterJson.getString("name")
        val id = afterJson.getString("id").toInt
        val age = afterJson.getString("age").toInt
        val email = afterJson.getString("email")
        val ts = item.ts_ms
        Student(id, name, age, email, qq, ts)
      })
    })
    result.write.format("hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL) //选择表的类型 到底是MERGE_ON_READ 还是 COPY_ON_WRITE
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "id") //设置主键
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "ts") //数据更新时间戳的
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "") //hudi分区列
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "") //hive对应的分区列
      .option("hoodie.table.name", "cdc_test_table") //hudi表名
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://hadoop101:10000") //hiveserver2地址
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default") //设置hudi与hive同步的数据库
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "cdc_test_table") //设置hudi与hive同步的表名
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY, classOf[NonpartitionedKeyGenerator].getName)
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[NonPartitionedExtractor].getName) // 没有分区
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true") //设置数据集注册并同步到hive
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true") //设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name()) //设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
      .option("hoodie.bulkinsert.shuffle.parallelism", "12")
      .option("hoodie.insert.shuffle.parallelism", "12")
      .option("hoodie.upsert.shuffle.parallelism", "12")
      .option("hoodie.delete.shuffle.parallelism", "12")
      .option("hoodie.bootstrap.parallelism", "12")
      .mode(SaveMode.Append)
      .save("/ss/hudi/cdc")
  }

  /**
   * 删除数据方法
   *
   * @param batchDF
   * @param sparkSession
   * @return
   */
  def deleteHudi(batchDF: Dataset[Model], sparkSession: SparkSession) = {
    import sparkSession.implicits._
    val result = batchDF.mapPartitions(partitions => {
      partitions.map(item => {
        val beforeData = item.beforeData
        val beforeJson = JSON.parseObject(beforeData)
        val qq = beforeJson.getString("qq")
        val name = beforeJson.getString("name")
        val id = beforeJson.getString("id").toInt
        val age = beforeJson.getString("age").toInt
        val email = beforeJson.getString("email")
        val ts = item.ts_ms
        Student(id, name, age, email, qq, ts)
      })
    })
    result.write.format("hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL) //选择表的类型 到底是MERGE_ON_READ 还是 COPY_ON_WRITE
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, "delete") //删除数据操作
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "id") //设置主键
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "ts") //数据更新时间戳的
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "") //hudi分区列
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "") //hive对应的分区列
      .option("hoodie.table.name", "cdc_test_table") //hudi表名
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://hadoop101:10000") //hiveserver2地址
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default") //设置hudi与hive同步的数据库
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "cdc_test_table") //设置hudi与hive同步的表名
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY, classOf[NonpartitionedKeyGenerator].getName)
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[NonPartitionedExtractor].getName) // 没有分区
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true") //设置数据集注册并同步到hive
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true") //设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name()) //设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
      .option("hoodie.bulkinsert.shuffle.parallelism", "12")
      .option("hoodie.insert.shuffle.parallelism", "12")
      .option("hoodie.upsert.shuffle.parallelism", "12")
      .option("hoodie.delete.shuffle.parallelism", "12")
      .option("hoodie.bootstrap.parallelism", "12")
      .mode(SaveMode.Append)
      .save("/ss/hudi/cdc")
  }
}
