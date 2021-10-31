package com.atguigu.hudi.test

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.index.HoodieIndex
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object TableTypeTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test_operator").setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    insertMergeOnReadTable(sparkSession)
    insertCopyOnReadTable(sparkSession)
  }

  def insertMergeOnReadTable(sparkSession: SparkSession) = {
    import org.apache.spark.sql.functions._
    val commitTime = System.currentTimeMillis().toString //生成提交时间
    val resultDF = sparkSession.read.json("/user/atguigu/ods/member.log")
      .withColumn("ts", lit(commitTime)) //添加ts时间戳
      .withColumn("hudipartition", concat_ws("/", col("dt"), col("dn"))) //添加分区 两个字段组合分区
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    resultDF.write.format("hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL) //选择表的类型 到底是MERGE_ON_READ 还是 COPY_ON_WRITE
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "uid") //设置主键
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "ts") //数据更新时间戳的
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "hudipartition") //hudi分区列
      .option("hoodie.table.name", "merge_member") //hudi表名
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://hadoop101:10000") //hiveserver2地址
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default") //设置hudi与hive同步的数据库
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "merge_member") //设置hudi与hive同步的表名
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "dt,dn") //hive表同步的分区列
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName) // 分区提取器 按/ 提取分区
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true") //设置数据集注册并同步到hive
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true") //设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name()) //设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
      .option("hoodie.insert.shuffle.parallelism", "12")
      .option("hoodie.upsert.shuffle.parallelism", "12")
      .mode(SaveMode.Append)
      .save("/user/atguigu/hudi/merge_test")
  }

  def insertCopyOnReadTable(sparkSession: SparkSession) = {
    import org.apache.spark.sql.functions._
    val commitTime = System.currentTimeMillis().toString //生成提交时间
    val resultDF = sparkSession.read.json("/user/atguigu/ods/member.log")
      .withColumn("ts", lit(commitTime)) //添加ts时间戳
      .withColumn("hudipartition", concat_ws("/", col("dt"), col("dn"))) //添加分区 两个字段组合分区
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    resultDF.write.format("hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL) //选择表的类型 到底是MERGE_ON_READ 还是 COPY_ON_WRITE
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "uid") //设置主键
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "ts") //数据更新时间戳的
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "hudipartition") //hudi分区列
      .option("hoodie.table.name", "copy_member") //hudi表名
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://hadoop101:10000") //hiveserver2地址
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default") //设置hudi与hive同步的数据库
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "copy_member") //设置hudi与hive同步的表名
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "dt,dn") //hive表同步的分区列
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName) // 分区提取器 按/ 提取分区
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true") //设置数据集注册并同步到hive
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true") //设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name()) //设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
      .option("hoodie.insert.shuffle.parallelism", "12")
      .option("hoodie.upsert.shuffle.parallelism", "12")
      .mode(SaveMode.Append)
      .save("/user/atguigu/hudi/copy_test")
  }

  def updateData(sparkSession: SparkSession) = {
    import org.apache.spark.sql.functions._
    val commitTime = System.currentTimeMillis().toString //生成提交时间
    val resultDF = sparkSession.read.json("/user/atguigu/ods/member.log")
      .withColumn("ts", lit(commitTime)) //添加ts时间戳
      .withColumn("hudipartition", concat_ws("/", col("dt"), col("dn")))
      .where("uid >=0  and uid <20")
      .withColumn("fullname", lit("hahahaha"))

    resultDF.write.format("hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL) //选择表的类型 到底是MERGE_ON_READ 还是 COPY_ON_WRITE
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "uid") //设置主键
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "ts") //数据更新时间戳的
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "hudipartition") //hudi分区列
      .option("hoodie.table.name", "merge_member") //hudi表名
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://hadoop101:10000") //hiveserver2地址
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default") //设置hudi与hive同步的数据库
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "merge_member") //设置hudi与hive同步的表名
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "dt,dn") //hive表同步的分区列
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName) // 分区提取器 按/ 提取分区
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true") //设置数据集注册并同步到hive
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true") //设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name()) //设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
      .option("hoodie.insert.shuffle.parallelism", "12")
      .option("hoodie.upsert.shuffle.parallelism", "12")
      .mode(SaveMode.Append)
      .save("/user/atguigu/hudi/merge_test")

    resultDF.write.format("hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL) //选择表的类型 到底是MERGE_ON_READ 还是 COPY_ON_WRITE
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "uid") //设置主键
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "ts") //数据更新时间戳的
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "hudipartition") //hudi分区列
      .option("hoodie.table.name", "copy_member") //hudi表名
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://hadoop101:10000") //hiveserver2地址
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default") //设置hudi与hive同步的数据库
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "copy_member") //设置hudi与hive同步的表名
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "dt,dn") //hive表同步的分区列
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName) // 分区提取器 按/ 提取分区
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true") //设置数据集注册并同步到hive
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true") //设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name()) //设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
      .option("hoodie.insert.shuffle.parallelism", "12")
      .option("hoodie.upsert.shuffle.parallelism", "12")
      .mode(SaveMode.Append)
      .save("/user/atguigu/hudi/copy_test")
  }
}
