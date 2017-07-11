package niki.spark.rdd

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by nikigoya on 7/10/2017.
  */
class SequenceFileExample {

  def writeSequenceFile(rdd : RDD[(String, Int)]) = {
    val datasetPath = System.getenv("DATA_SETS_PATH")
    rdd.saveAsSequenceFile(datasetPath + "seq_output")
  }

  def readSequenceFile(sc:SparkContext) = {
    val rdd = sc.sequenceFile(System.getenv("DATA_SETS_PATH") + "seq_output/part*", classOf[Text],
      classOf[IntWritable])
    rdd.foreach(println)
  }
}

object SequenceFileExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("UserStoryDemo")
    conf.setMaster("local[1]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(("A", 1 ), ("B", 2) , ("C", 3)))

    val sequenceFileExample = new SequenceFileExample
    sequenceFileExample.writeSequenceFile(rdd)
    sequenceFileExample.readSequenceFile(sc)
  }
}