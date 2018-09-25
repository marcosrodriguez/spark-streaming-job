package com.uc3m.historify

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object Historify {

  def main(args: Array[String]): Unit = {

    implicit val sparks: SparkSession = SparkSession
      .builder()
      .appName("CassandraToHDFS")
      .config("spark.cassandra.connection.host", "localhost:9042")
      .getOrCreate()


    val movementsdf = sparks.sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "movements",
          "keyspace" -> "uc3m")
      )
      .load
      .filter(col("transaction_date") between (date_sub(current_timestamp(), 1), current_timestamp()))

    movementsdf.write.mode(SaveMode.Append).partitionBy("iban", "client_id").saveAsTable("movements")

    val contractsdf = sparks.sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "contracts",
          "keyspace" -> "uc3m")
      )
      .load
      .filter(col("open_date") between (date_sub(current_timestamp(), 1), current_timestamp()))

    contractsdf.write.mode(SaveMode.Append).partitionBy("iban").saveAsTable("contracts")

    val clientsdf = sparks.sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "clients",
          "keyspace" -> "uc3m")
      )
      .load
      .filter(col("open_date") between (date_sub(current_timestamp(), 1), current_timestamp()))

    clientsdf.write.mode(SaveMode.Append).partitionBy("client_id").saveAsTable("clients")

    val relationsdf = sparks.sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "client_contract_relations",
          "keyspace" -> "uc3m")
      )
      .load
      .filter(col("open_date") between (date_sub(current_timestamp(), 1), current_timestamp()))

    relationsdf.write.mode(SaveMode.Append).partitionBy("client_id", "iban").saveAsTable("client_contract_relations")

    val balancesdf = sparks.sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(
        Map(
          "table" -> "balances",
          "keyspace" -> "uc3m")
      )
      .load
      .filter(col("transaction_date") between (date_sub(current_timestamp(), 1), current_timestamp()))

    balancesdf.write.mode(SaveMode.Append).partitionBy("client_id", "iban").saveAsTable("balances")

  }

}
