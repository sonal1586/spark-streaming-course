package com.sundogsoftware.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._
import java.util.concurrent._
import java.util.concurrent.atomic._

object AverageTweetLength {
  
  def main(args: Array[String]) {
    
    setupTwitter()
    
    val ssc = new StreamingContext("local[*]", "AverageTweetLength", Seconds(1))
    
    setupLogging()
    
    val tweets = TwitterUtils.createStream(ssc, None)
    
    val rddDStream = tweets.map(x => x.getText())
    
    //rddDStream.print()
    
    val length = rddDStream.map(x => x.length())
    
    //length.print()
    
    var totalTweets = new AtomicLong()
    val totalChars = new AtomicLong()
    
    length.foreachRDD((rdd, time) => {
      
      var count = rdd.count()
      if (count > 0) {
        totalTweets.getAndAdd(count)
        
        totalChars.getAndAdd(rdd.reduce((x,y) => x + y))
        
        println("Total tweets: " + totalTweets.get() + 
            " Total characters: " + totalChars.get() + 
            " Average: " + totalChars.get() / totalTweets.get())
      }
    })  
    
    ssc.start()
    ssc.awaitTermination()
    
    
  }
  
}