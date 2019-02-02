package com.sundogsoftware.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._


object PrintTweets2 {
  
  def main(args: Array[String]) {
    
    setupTwitter()
    
    val ssc = new StreamingContext("local[*]", "PrintTweets2",Seconds(1))
    
    setupLogging()
    
    val tweets = TwitterUtils.createStream(ssc, None)
    val results = tweets.map(x => x.getText()).flatMap(spl => spl.split(" "))
    val filtered = results.filter(x => x.startsWith("#"))
    
    val hashtagKeyValues = filtered.map(hashtag => (hashtag, 1))
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))
    
    //results.saveAsTextFiles("Tweets", "txt")

    hashtagKeyValues.print()
    
    
    ssc.start()
    ssc.awaitTermination()
  }
}