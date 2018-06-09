package com.jp.sql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import com.jp.utils.StringUtils._


object UserResults {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println(
        """
          | dataInputPath 文件输入路径
        """.stripMargin)
      sys.exit()
    }
    val Array(dataInputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
    val sc = new SparkContext(conf)
    val rawData = sc.textFile(dataInputPath)
    println("原始数据：")
    rawData.collect.foreach(println)

    val row = rawData.map(line => {
      val str = line.split(",", -1)
      Row(
        str(7).toStringPlus,
        str(1).toStringPlus,
        str(0).toStringPlus,
        str(6).toIntPlus
      )
    })

    val schema = StructType(Seq(
      StructField("user_id", StringType),
      StructField("username",StringType),
      StructField("exam_name", StringType),
      StructField("result", IntegerType)
    ))

    val sQLContext = new SQLContext(sc)
    val frame = sQLContext.createDataFrame(row, schema)
    frame.createTempView("UserNameResult")

    val result:DataFrame = sQLContext.sql("SELECT user_id,first(username) username,sum(if(result=1,1,0)) correct,count(*) total from UserNameResult group by user_id")

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "")
    prop.put("driver","com.mysql.jdbc.Driver")

    result.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/web", "analysis_score_per_user", prop)

    println("数据已存储到 web 数据库 ，analysis_score_per_user 表中。")
    sc.stop()
  }
}
