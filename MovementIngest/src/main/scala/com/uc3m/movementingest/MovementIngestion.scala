package com.uc3m.movementingest

import com.uc3m.movementingest.reader.KafkaStreamReader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MovementIngestion {

  def main(args: Array[String]): Unit = {
    implicit val sparks: SparkSession = SparkSession.builder().appName("KafkaStreamProcessor").getOrCreate()
    implicit val ssc = new StreamingContext(sparks.sparkContext, Seconds(5))
    val bootstrapServers: String = "localhost:9092, localhost:9093, localhost:9094"
    val topics: String = "movements_topic"
    val groupId: String = "movements_group_id"
    val dataschema : StructType = StructType(
      List(
        StructField("client_id", StringType),
        StructField("iban", StringType),
        StructField("amount", DoubleType),
        StructField("transaction_date", TimestampType),
        StructField("concept", StringType),
        StructField("balance", DoubleType)
      )
    )

    val messages = KafkaStreamReader.readStream(bootstrapServers, topics, groupId)
    messages.foreachRDD { rdd =>
      if (rdd.toLocalIterator.nonEmpty) {
        val rowsRDD : RDD[Row] = rdd.map(record => record.value().split(";")).map(arr => Row.fromSeq(arr))
        val df : DataFrame = sparks.createDataFrame(rowsRDD, dataschema)
        df.write
          .format("org.apache.spark.sql.cassandra")
          .options(
            Map(
              "table" -> "movements",
              "keyspace" -> "uc3m")
          )
          .save()
        val balancesDF: DataFrame = df.select("client_id", "iban", "transaction_date", "balance")
          .orderBy("transaction_date")
        balancesDF.write
          .format("org.apache.spark.sql.cassandra")
          .options(
            Map(
              "table" -> "balances",
              "keyspace" -> "uc3m")
          )
          .save()
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
