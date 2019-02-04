package com.sundogsoftware.sparkstreaming 


 import org.apache.log4j._
 import org.apache.spark.SparkConf
 import org.apache.spark.streaming._
 import org.apache.spark.streaming.StreamingContext._
 import org.apache.spark.streaming.Seconds._

 import java.util.regex.Pattern
 import java.util.regex.Matcher
 
 import Utilities._
 import org.apache.spark.storage.StorageLevel


 object LorParser {
   
   def main(args: Array[String])
   {
     setupTwitter()
     
     val ssc = new StreamingContext("local[*]","LogParser",Seconds(1))
     
     setupLogging()
     
     val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK)
     
     val pattern = apacheLogPattern()
     
     val request = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if(matcher.matches()) matcher.group(5) })
     
     val url = request.map(x => {val arr = x.toString().split(" "); if(arr.length == 3) arr(1) else "[Error]" })
     
     val urlCounts = url.map(x => (x,1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(5), Seconds(1))
     
     val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x=>x._2, false))
     
     sortedResults.print()
     
    ssc.checkpoint("../checkpoint/")
    ssc.start()
    ssc.awaitTermination()
     
     
     
     
     
     
   }
 }