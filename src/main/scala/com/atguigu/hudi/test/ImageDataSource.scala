package com.atguigu.hudi.test

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object ImageDataSource {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf = new SparkConf().setAppName("dwd_member_import")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
//    readImageToHdfs(sparkSession)
    readImageFromHdfs(sparkSession)
  }

  def readImageToHdfs(sparkSession: SparkSession): Unit = {
    import org.apache.spark.sql.functions._
    val commitTime = System.currentTimeMillis().toString //生成提交时间
    val dt = DateTimeFormatter.ofPattern("yyyy/MM/dd").format(LocalDateTime.now())
    val df = sparkSession.read.format("image").option("dropInvalid", "true") //dropInvalid 是否删除无效图片
      .load("file://" + this.getClass.getResource("/").toURI.getPath)
    df.printSchema()
    val result = df.select("image.origin", "image.width", "image.height", "image.nChannels", "image.mode", "image.data")
      .withColumn("dt", lit(dt))
      .withColumn("ts", lit(commitTime))
      .withColumn("uuid", col("origin"))
    result.write.format("org.apache.hudi")
      .option("hoodie.insert.shuffle.parallelism", 12)
      .option("hoodie.upsert.shuffle.parallelism", 12)
      .option("PRECOMBINE_FIELD_OPT_KEY", "ts") //指定提交时间列
      .option("RECORDKEY_FIELD_OPT_KEY", "uuid") //指定唯一标示列
      .option("hoodie.table.name", "testTable")
      .option("hoodie.datasource.write.partitionpath.field", "dt") //分区列
      .mode(SaveMode.Overwrite)
      .save("/user/atguigu/hudi/image")
  }

  def readImageFromHdfs(sparkSession: SparkSession): Unit = {
    val df=sparkSession.read.format("org.apache.hudi")
      .load("/user/atguigu/hudi/image/*/*/*")
    df.show()

  }
}
