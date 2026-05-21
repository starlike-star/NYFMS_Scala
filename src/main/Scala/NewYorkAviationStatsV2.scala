import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.{Connection, DriverManager, PreparedStatement}

object NewYorkAviationStatsV2 {
  def main(args: Array[String]): Unit = {

    //引擎初始化

    val spark = SparkSession.builder()
      .appName("NY_Aviation_Intelligence_V2")
      .master("local[*]")
      .getOrCreate()

    spark.conf.set("spark.sql.repl.eagerEval.enabled", true)

    //数据读取与清洗
    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/flights_sample_3m.csv")

    val cleanDF = rawDF
      .filter(col("CANCELLED") === 0.0) // 过滤取消航班
      .withColumn("FL_YEAR", year(col("FL_DATE")))
      .withColumn("FL_MONTH", month(col("FL_DATE")))
      // 核心：将延误字段的 NULL 填充为 0.0，防止聚合与数据库写入时报错
      .na.fill(0.0, Seq("DEP_DELAY", "ARR_DELAY", "DELAY_DUE_CARRIER",
        "DELAY_DUE_WEATHER", "DELAY_DUE_NAS"))

    cleanDF.createOrReplaceTempView("flights_data")

    //核心业务 SQL

    // [保留V1.0]：出港基础吞吐量
    val nyOutboundStats = spark.sql(
      """
        |SELECT 
        |   FL_YEAR, FL_MONTH, DEST as dest_airport, COUNT(1) as flight_count
        |FROM flights_data
        |WHERE ORIGIN IN ('JFK', 'LGA', 'EWR')
        |GROUP BY FL_YEAR, FL_MONTH, DEST
      """.stripMargin)

    // [新增 V2.0 业务一]：目的地航线服务质量 (准点率与平均延误)
    val otpStats = spark.sql(
      """
        |SELECT 
        |   FL_YEAR, FL_MONTH, DEST as dest_airport,
        |   SUM(CASE WHEN DEP_DELAY <= 15.0 THEN 1 ELSE 0 END) / COUNT(1) as on_time_rate,
        |   AVG(CASE WHEN DEP_DELAY > 0.0 THEN DEP_DELAY ELSE null END) as avg_delay_mins
        |FROM flights_data
        |WHERE ORIGIN IN ('JFK', 'LGA', 'EWR')
        |GROUP BY FL_YEAR, FL_MONTH, DEST
      """.stripMargin)

    // [新增 V2.0 业务二]：航空巨头竞争格局 (市场份额与质量)
    val carrierStats = spark.sql(
      """
        |SELECT 
        |   FL_YEAR, FL_MONTH, AIRLINE_CODE, MAX(AIRLINE) as airline_name,
        |   COUNT(1) as flight_volume,
        |   SUM(CASE WHEN DEP_DELAY <= 15.0 THEN 1 ELSE 0 END) / COUNT(1) as on_time_rate
        |FROM flights_data
        |WHERE ORIGIN IN ('JFK', 'LGA', 'EWR') OR DEST IN ('JFK', 'LGA', 'EWR')
        |GROUP BY FL_YEAR, FL_MONTH, AIRLINE_CODE
      """.stripMargin)

    // [新增 V2.0 业务三]：黑天鹅事件全网延误归因
    val rootCauseStats = spark.sql(
      """
        |SELECT 
        |   FL_YEAR, FL_MONTH,
        |   SUM(DELAY_DUE_CARRIER) as sum_carrier_delay,
        |   SUM(DELAY_DUE_WEATHER) as sum_weather_delay,
        |   SUM(DELAY_DUE_NAS) as sum_nas_delay
        |FROM flights_data
        |WHERE DEP_DELAY > 15.0 AND (ORIGIN IN ('JFK', 'LGA', 'EWR') OR DEST IN ('JFK', 'LGA', 'EWR'))
        |GROUP BY FL_YEAR, FL_MONTH
      """.stripMargin)

    // 4. 持久化入库
    val jdbcUrl = "jdbc:mysql://localhost:3306/flight_stats?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&characterEncoding=utf8"
    val dbUser = "root"
    val dbPassword = "xu20051203" // 请确保这里的密码是你本地的 MySQL 密码

    println("\n>>> 开始执行数据库写入计划...")

    // 写入 1: 基础出港吞吐量
    nyOutboundStats.foreachPartition((partition: Iterator[Row]) => {
      var conn: Connection = null; var ps: PreparedStatement = null
      try {
        conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword); conn.setAutoCommit(false)
        val sql = "REPLACE INTO ny_outbound_stats (flight_year, flight_month, dest_airport, flight_count) VALUES (?, ?, ?, ?)"
        ps = conn.prepareStatement(sql)
        partition.foreach(row => {
          ps.setInt(1, row.getAs[Int]("FL_YEAR"))
          ps.setInt(2, row.getAs[Int]("FL_MONTH"))
          ps.setString(3, row.getAs[String]("dest_airport"))
          ps.setLong(4, row.getAs[Long]("flight_count"))
          ps.addBatch()
        })
        ps.executeBatch(); conn.commit()
      } catch { case e: Exception => e.printStackTrace() }
      finally { if (ps != null) ps.close(); if (conn != null) conn.close() }
    })

    // 写入 2: 航线服务质量 (OTP)
    otpStats.foreachPartition((partition: Iterator[Row]) => {
      var conn: Connection = null; var ps: PreparedStatement = null
      try {
        conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword); conn.setAutoCommit(false)
        val sql = "REPLACE INTO ny_otp_stats (flight_year, flight_month, dest_airport, on_time_rate, avg_delay_mins) VALUES (?, ?, ?, ?, ?)"
        ps = conn.prepareStatement(sql)
        partition.foreach(row => {
          ps.setInt(1, row.getAs[Int]("FL_YEAR"))
          ps.setInt(2, row.getAs[Int]("FL_MONTH"))
          ps.setString(3, row.getAs[String]("dest_airport"))
          // 注意：可能存在某条航线某个月全是准点（没有平均延误时间），所以要做个判空
          ps.setDouble(4, if (row.isNullAt(3)) 1.0 else row.getAs[Double]("on_time_rate"))
          ps.setDouble(5, if (row.isNullAt(4)) 0.0 else row.getAs[Double]("avg_delay_mins"))
          ps.addBatch()
        })
        ps.executeBatch(); conn.commit()
      } catch { case e: Exception => e.printStackTrace() }
      finally { if (ps != null) ps.close(); if (conn != null) conn.close() }
    })

    // 写入 3: 航司竞争格局 (Carrier)
    carrierStats.foreachPartition((partition: Iterator[Row]) => {
      var conn: Connection = null; var ps: PreparedStatement = null
      try {
        conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword); conn.setAutoCommit(false)
        val sql = "REPLACE INTO ny_carrier_stats (flight_year, flight_month, airline_code, airline_name, flight_volume, on_time_rate) VALUES (?, ?, ?, ?, ?, ?)"
        ps = conn.prepareStatement(sql)
        partition.foreach(row => {
          ps.setInt(1, row.getAs[Int]("FL_YEAR"))
          ps.setInt(2, row.getAs[Int]("FL_MONTH"))
          ps.setString(3, row.getAs[String]("AIRLINE_CODE"))
          ps.setString(4, row.getAs[String]("airline_name"))
          ps.setLong(5, row.getAs[Long]("flight_volume"))
          ps.setDouble(6, if (row.isNullAt(5)) 1.0 else row.getAs[Double]("on_time_rate"))
          ps.addBatch()
        })
        ps.executeBatch(); conn.commit()
      } catch { case e: Exception => e.printStackTrace() }
      finally { if (ps != null) ps.close(); if (conn != null) conn.close() }
    })

    // 写入 4: 极端延误归因 (Root Cause)
    rootCauseStats.foreachPartition((partition: Iterator[Row]) => {
      var conn: Connection = null; var ps: PreparedStatement = null
      try {
        conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword); conn.setAutoCommit(false)
        val sql = "REPLACE INTO ny_root_cause_stats (flight_year, flight_month, sum_carrier_delay, sum_weather_delay, sum_nas_delay) VALUES (?, ?, ?, ?, ?)"
        ps = conn.prepareStatement(sql)
        partition.foreach(row => {
          ps.setInt(1, row.getAs[Int]("FL_YEAR"))
          ps.setInt(2, row.getAs[Int]("FL_MONTH"))
          ps.setDouble(3, if (row.isNullAt(2)) 0.0 else row.getAs[Double]("sum_carrier_delay"))
          ps.setDouble(4, if (row.isNullAt(3)) 0.0 else row.getAs[Double]("sum_weather_delay"))
          ps.setDouble(5, if (row.isNullAt(4)) 0.0 else row.getAs[Double]("sum_nas_delay"))
          ps.addBatch()
        })
        ps.executeBatch(); conn.commit()
      } catch { case e: Exception => e.printStackTrace() }
      finally { if (ps != null) ps.close(); if (conn != null) conn.close() }
    })

    println("\n====== V2.0 运行完毕 ======")
    println("所有进出港吞吐量、准点率、航司竞争与延误归因数据已全部安全写入 MySQL！")
    spark.stop()
  }
}
