package niki.spark.rdd

/**
  * Created by nikigoya on 7/9/2017.
  */

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by nikigoya on 7/9/2017.
  */
object UserStoryDemo {

  def parse_N_calculate_age(row: String) = {
    val columns = row.split('|')
    /**
      *  userid,age,gender,occupation,zip = data.split("|")
             return  userid, age_group(int(age)),gender,occupation,zip,int(age)
      */
    (columns(0), age_group((columns(1).toInt)), columns(2), columns(3), columns(4), columns(1).toInt)
  }

  def  age_group(age: Int): String ={
    if (age < 10)
      "0-10"
    else if (age < 20){
      "10-20"
    } else if (age < 30){
      "20-30"
    } else if (age < 40) {
      "30-40"
    } else
    "above 40"
  }



  def main(args: Array[String]): Unit = {
    val datasetPath = System.getenv("DATA_SETS_PATH")
    val filePath = datasetPath.concat("u.user")
    val conf = new SparkConf()
    conf.setAppName("UserStoryDemo")
    conf.setMaster("local[1]")
    val sc = new SparkContext(conf)

    val userRdd = sc.textFile(filePath)

    println("No. of users: " , userRdd.count() )

    val data_with_age_bucket  = userRdd.map(row => parse_N_calculate_age(row))

    data_with_age_bucket.take(10).foreach(println)

    val rdd20_30 = data_with_age_bucket.filter(f => f._2.contains("20-30"))

    println("User within 20-30 ", rdd20_30.count())

    println("Count by value -> ")
    rdd20_30.map(f => f._3).countByValue().foreach(println)

    rdd20_30.unpersist();

    var Under_age = sc.accumulator(0)

    var Over_age = sc.accumulator(0)
    def outliers(agegrp : String) = {
      if(agegrp.equals("above 40")) {
        Over_age +=  1
      }
      if(agegrp.equals("0-10")) {
        Under_age +=  1
      }
    }
    data_with_age_bucket.map(f => f._2).countByValue().foreach(println)
    val df = data_with_age_bucket.map( r => outliers(r._2)).collect()

    println(s"Over_age = $Over_age")
    println(s"Under_age = $Under_age")
  }

}
