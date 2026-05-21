import org.apache.spark.{SparkConf, SparkContext}
import java.sql.{Connection, DriverManager, PreparedStatement}

// 数据结构增加 month
case class NYFlight(
                     date: String, year: Int, month: Int, airlineCode: String,
                     origin: String, dest: String, depTime: String, depDelay: Double, isCancelled: Boolean
                   )

object NewYorkAviationStats {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NewYorkAviation").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rawRDD = sc.textFile("data/flights_sample_3m.csv")
    val csvRegex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"

    // 1. 数据解析 (提取月份)
    val flightRDD = rawRDD.mapPartitionsWithIndex { (idx, iter) =>
      if (idx == 0) iter.drop(1) else iter
    }.flatMap { line =>
      try {
        val cols = line.split(csvRegex, -1).map(_.trim.replaceAll("^\"|\"$", ""))
        val date = cols(0) // 格式如 2019-01-09
        val year = date.substring(0, 4).toInt
        val month = date.substring(5, 7).toInt // 提取月份
        val origin = cols(6)
        val dest = cols(8)
        val isCancelled = if (cols(20) == "1.0") true else false

        Some(NYFlight(date, year, month, cols(3), origin, dest, cols(11), 0.0, isCancelled))
      } catch {
        case e: Exception => None
      }
    }

    val nyAirports = Set("JFK", "LGA", "EWR")

    // 2. 统计出港 (纽约飞出) - Key 为 (年份, 月份, 目的地)
    val nyOutboundStats = flightRDD
      .filter(f => nyAirports.contains(f.origin) && !f.isCancelled)
      .map(f => ((f.year, f.month, f.dest), 1))
      .reduceByKey(_ + _)

    // 3. 统计进港 (飞往纽约) - Key 为 (年份, 月份, 出发地)
    val nyInboundStats = flightRDD
      .filter(f => nyAirports.contains(f.dest) && !f.isCancelled)
      .map(f => ((f.year, f.month, f.origin), 1))
      .reduceByKey(_ + _)

    // 数据库连接配置 (注意你的密码和 allowPublicKeyRetrieval 参数)
    val jdbcUrl = "jdbc:mysql://localhost:3306/flight_stats?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&characterEncoding=utf8"
    val dbUser = "root"
    val dbPassword = "xu20051203"

    // 4. 写入出港数据
    nyOutboundStats.foreachPartition(partition => {
      var conn: Connection = null; var ps: PreparedStatement = null
      try {
        conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)
        conn.setAutoCommit(false)
        val sql = "REPLACE INTO ny_outbound_stats (flight_year, flight_month, dest_airport, flight_count) VALUES (?, ?, ?, ?)"
        ps = conn.prepareStatement(sql)
        partition.foreach(record => {
          ps.setInt(1, record._1._1) // year
          ps.setInt(2, record._1._2) // month
          ps.setString(3, record._1._3) // dest
          ps.setInt(4, record._2)    // count
          ps.addBatch()
        })
        ps.executeBatch()
        conn.commit()
      } catch { case e: Exception => e.printStackTrace() }
      finally { if (ps != null) ps.close(); if (conn != null) conn.close() }
    })

    // 5. 写入进港数据 (逻辑同上)
    nyInboundStats.foreachPartition(partition => {
      var conn: Connection = null; var ps: PreparedStatement = null
      try {
        conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)
        conn.setAutoCommit(false)
        val sql = "REPLACE INTO ny_inbound_stats (flight_year, flight_month, origin_airport, flight_count) VALUES (?, ?, ?, ?)"
        ps = conn.prepareStatement(sql)
        partition.foreach(record => {
          ps.setInt(1, record._1._1)
          ps.setInt(2, record._1._2)
          ps.setString(3, record._1._3) // origin
          ps.setInt(4, record._2)
          ps.addBatch()
        })
        ps.executeBatch()
        conn.commit()
      } catch { case e: Exception => e.printStackTrace() }
      finally { if (ps != null) ps.close(); if (conn != null) conn.close() }
    })

    println("完美！带有时间维度的进出港双向数据均已入库！")
    sc.stop()
  }
}