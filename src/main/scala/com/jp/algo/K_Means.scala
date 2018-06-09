package com.jp.algo


import java.util.Properties
import org.apache.spark.mllib.linalg._

import org.apache.spark.mllib.clustering._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

import Array._
import scala.collection.mutable.ArrayBuffer

object K_Means {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("K-MeansCluster").setMaster("local")
    val sc = new SparkContext(conf)
    val path = "G:\\bishe\\anylize\\kddcup.data_10_percent_corrected"
    val rawData = sc.textFile(path)
    println("初始数据：")
    rawData.collect.foreach(println)

    //    println("全部类型及次数：")
    //    rawData.map(_.split(',').last).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)

    val labelsAndData = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      val label = buffer.remove(buffer.length - 1)
      val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
      (label, vector)
    }
    val data = labelsAndData.values.cache()

    println("从10到30簇进行聚类处理")
    println("（簇数，标准差）")
    val tempResult = ArrayBuffer[(Int, Double)]()
    (10 to 30 ).map(k => (k, clusteringScore(data, k))).foreach(i => {
      tempResult += i
      println(i)
    })
//    val limit = 500
//    val k = chooseK(tempResult, limit)
//    if (k == -1) {
//      println("聚类出错！")
//      sys.exit(0)
//    }
//    println("当limit为：" + limit + ",选择K为：" + k)
//
//    val model = new KMeans().setK(k).setMaxIterations(1000).setSeed(6666666666L).run(data)
//
//    val clusterLabelResult = labelsAndData.map { case (label, datum) =>
//      val cluster = model.predict(datum) //预测的结果为相应的结果集
//      (cluster + 1, label)
//    }
//
//    clusterLabelResult.sortByKey().foreach { case (cluster, label) =>
//      println(f"$cluster%1s$label%20s")
//    }
//
//
//    val clusterLabelCount = labelsAndData.map { case (label, datum) =>
//      val cluster = model.predict(datum)
//      (cluster, label)
//    }.countByValue
//    clusterLabelCount.toSeq.sorted.foreach {
//      case ((cluster, label), count) =>
//        println(f"$cluster%1s$label%18s$count%8s")
//    }
  }

  //    计算两点距离函数：
  def distance(a: Vector, b: Vector) =
    math.sqrt(a.toArray.zip(b.toArray).
      map(p => p._1 - p._2).map(d => d * d).sum)

  //    计算数据点到簇质心距离函数：
  def distToCentroid(datum: Vector, model: KMeansModel) = {
    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster)
    distance(centroid, datum)
  }

  //    给定k值的模型的平均质心距离函数：
  def clusteringScore(data: RDD[Vector], k: Int) = {
    val seed = 6666666666L
    val kmeans = new KMeans().setMaxIterations(1000)
    kmeans.setSeed(seed)
    kmeans.setK(k)
    val model = kmeans.run(data)
    data.map(datum => distToCentroid(datum, model)).mean()
  }

  def chooseK(arrayBuffer: ArrayBuffer[(Int, Double)], limit: Double): Int = {
    val arr = arrayBuffer.toArray
    for (i <- 1 to (arr.length - 1)) {
      if ((arr(i - 1)._2) - (arr(i)._2) <= limit) {
        return arr(i - 1)._1
      }
    }
    return -1
  }

}
