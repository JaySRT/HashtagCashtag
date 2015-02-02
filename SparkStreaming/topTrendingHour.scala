import java.util.Properties

import kafka.producer._

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.Logging
import org.apache.log4j.{Level, Logger}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.joda.time._
import org.joda.time.DateTime
import org.joda.time.format._
import java.io.PrintWriter

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

object twitterGetCount
{
		val positive = Set(
		"upgrade",
		"upgraded",
		"long",
		"buy",
		"buying",
		"growth",
		"good",
		"gained",
		"well",
		"great",
		"nice",
		"top",
		"support",
		"update",
		"strong",
		"bullish",
		"bull",
		"highs",
		"win",
		"positive",
		"profits",
		"bonus",
		"potential",
		"success",
		"winner",
		"winning",
		"good")


	val negative =Set(
		"downgraded",
		"bears",
		"bear",
		"bearish",
		"volatile",
		"short",
		"sell",
		"selling",
		"forget",
		"down",
		"resistance",
		"sold",
		"sellers",
		"negative",
		"selling",
		"blowout",
		"losses",
		"war",
		"lost",
		"loser")
	
	def getWordSentiment(word:String)=
	{
		if (positive.contains(word)) 1 
		else if (negative.contains(word)) -1 
		else 0
	}

    val patternWord = "\\W|\\s|\\d"
    val patternTicker = "\\$[A-Z]+".r
    
    def getMonth(month:String)=
    { 
        val m = month.toUpperCase() match
        {
            case "JAN"=>1
            case "FEB"=>2
            case "MAR"=>3
            case "APR"=>4
            case "MAY"=>5
            case "JUN"=>6
            case "JUL"=>7
            case "AUG"=>8
            case "SEP"=>9
            case "OCT"=>10
            case "NOV"=>11
            case "DEC"=>12
    
        }
        m.toString
    }
    def getTime(dateString:String) = 
    {
        val str = dateString.split(' ')
        val Month = getMonth(str(1))
        val Day = str(2)
        val Year = str(5).split('"')(0)
        val time = str(3).split(':')
        val Hr = time(0)
        val Min = time(1)
        val Sec = time(2)

        ( Year, Month, Day, Hr, Min, Sec)

    }

    def getWeek(year:String, month:String, day:String)=
    {
        val date = new DateTime(year.toInt, month.toInt, day.toInt, 12, 0, 0, 0)
        date.getWeekyear().toString+'-'+date.getWeekOfWeekyear().toString
    }
    
    def updateFunc(values: Seq[(Int,Int)], runningCount: Option[(Int,Int)]):
      Option[(Int,Int)] = {
        val newCount1 = values.map(x=>x._1).sum //foldLeft(0)(_ + _)
        val newCount2 = values.map(x=>x._2).sum //foldLeft(0)(_ + _)

        val (oldCount1,oldCount2) = runningCount.getOrElse((0,0))
       Some((newCount1 + oldCount1, newCount2 + oldCount2)) 
    }

    def getResult(granularity:String)=
    {
        //val confSparkCassandra  = new SparkConf(true)
        //        .setAppName("Twitter Streaming Series")
        //        .set("spark.cassandra.connection.host", "54.67.105.220")
        val sparkConf = new SparkConf().setAppName("twitter streaming count")
        //val ssc = new StreamingContext(confSparkCassandra, Seconds(60))
        val ssc = new StreamingContext(sparkConf, Seconds(10))
        ssc.checkpoint("Top trending hour")

        // create Kakfa stream
        // Set up the input DStream to read from Kafka (in parallel)
        val zkQuorum = "localhost:2181"
        val group  = "SparkStreamingTopTrending"
        val inputTopic = "twitterStream"
        val topicMap =  Map(inputTopic -> 1)
        val numPartitionsOfInputTopic = 1
  
        // create a DStream from Kafka
        val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

        val tweets = lines.map( x=> parse(x))
        

        // get date
        val date = tweets.map(x=> ( getTime(compact(render(x \ "created_at" ))), compact(render(x \ "text")) )  )
    			.map{case((a,b,c,d,e,f),text)=>(a,b,c,d,e,f,text)}
        //val date = dateString.map(x=>getTime(x))

        // get texts
        val texts = tweets.map(x=>compact(render(x \ "text")))
        val timeStep = granularity match 
        {
          case "YEAR" =>
            date map {case(a,b,c,d,e,f,text)=>(a,text)}
          case "MONTH" =>
            date map {case(a,b,c,d,e,f,text)=>(a+'-'+b,text)}
          case "WEEK"=>
            date map {case(a,b,c,d,e,f,text)=>(getWeek(a,b,c),text)}
          case "DAY" =>
            date map {case(a,b,c,d,e,f,text)=>(a+'-'+b+'-'+c,text)}
          case "HR" =>
            date map {case(a,b,c,d,e,f,text)=>(a+'-'+b+'-'+c+'-'+d,text)}      
          case "MIN" =>
            date map {case(a,b,c,d,e,f,text)=>(a+'-'+b+'-'+c+'-'+d+'-'+e,text)}
          case "SEC" =>
            date map {case(a,b,c,d,e,f,text)=>(a+'-'+b+'-'+c+'-'+d+'-'+e+'-'+f,text)}         
        }

        // ticker frequency
        val mapResult = timeStep flatMap{ case(a,b)=> (patternTicker findAllIn b).toList.map(l=>((a,l),1))}
        //val tickerFrequency = mapResult.updateStateByKey[Int](updateFunc _)

        // ticker sentiment
        val words = timeStep flatMap{ case(a,b)=> (b.trim().toLowerCase().split(patternWord)).map(c=>((a,b),getWordSentiment(c))) }
        val sentiment = words.reduceByKey(_+_)
        val tickerSentiment = sentiment.flatMap{ case((a,b),c) =>  (patternTicker findAllIn b).toList.map(l=>((a,l),c)) } //.reduceByKey(_+_)
        
        val pair1 = mapResult join tickerSentiment

        val lastHourPair = pair1.map{ case((date,ticker),(frequency,sentiment)) => (ticker, (frequency, sentiment) }

        
        lastHourPair = reduceByKeyAndWindow((x,y)=>(x._1+y._1,x._2+y._2),Seconds(1800),Seconds(30))

        val result = lastHourPair.map(case{ticker,(frequency, sentiment)}=>(frequency,(ticker,sentiment)))
                    .sortByKey(false).map{ case(frequency,(ticker,sentiment))=>(ticker.split('$')(1), frequency, sentiment) }

        
        result.print



    //    val pair2 = pair1.reduceByKey{case(x,y)=>(x._1+x._2, y._1+y._2)}
    //val resultPair = pair1.updateStateByKey[(Int,Int)](updateFunc _)
      //  val result = resultPair.map{case((date,ticker),(frequency,sentiment))=>(date,frequency,ticker.split('$')(1),sentiment)}

    // join both table
    //val resultPair = tickerFrequency.join(tickerSentiment).map{case((a,b),(c,d))=>((a,c),(b,d))}
// val resultMin = result.map{case(date, frequency, ticker, sentiment)=> (date.split('-')(0).toInt, date.split('-')(1).toInt, date.split('-')(2).toInt, date.split('-')(3).toInt, date.split('-')(4).toInt, frequency, ticker, sentiment)}
//resultMin.print()
    //tickerFrequency.print()

   /*     granularity match 
        {
            case "YEAR" =>
                val resultYear = result.map{case(date, frequency, ticker, sentiment)=> (date.toInt, frequency, ticker, sentiment)} // keyspace, table, column names
                resultYear.saveToCassandra("twitterseriesstreaming", "trendingyear", SomeColumns("year", "frequency", "ticker", "sentiment"))            
            case "MONTH" =>
                val resultMonth = result.map{case(date, frequency, ticker, sentiment)=> (date.split('-')(0).toInt, date.split('-')(1).toInt, frequency, ticker, sentiment)}
                resultMonth.saveToCassandra("twitterseriesstreaming", "trendingmonth", SomeColumns("year", "month", "frequency", "ticker", "sentiment"))         
            case "WEEK"=>
                val resultWeek = result.map{case(date, frequency, ticker, sentiment)=> (date.split('-')(0).toInt, date.split('-')(1).toInt, frequency, ticker, sentiment)}
                resultWeek.saveToCassandra("twitterseriesstreaming", "trendingweek", SomeColumns("year", "week", "frequency", "ticker", "sentiment"))            
            case "DAY" =>
                val resultDay = result.map{case(date, frequency, ticker, sentiment)=> (date.split('-')(0).toInt, date.split('-')(1).toInt, date.split('-')(2).toInt, frequency, ticker, sentiment)}
//                resultDay foreach println
                resultDay.saveToCassandra("twitterseriesstreaming", "trendingday", SomeColumns("year", "month", "day", "frequency", "ticker", "sentiment"))          
            case "HR" =>
                val resultHr = result.map{case(date, frequency, ticker, sentiment)=> (date.split('-')(0).toInt, date.split('-')(1).toInt, date.split('-')(2).toInt, date.split('-')(3).toInt, frequency, ticker, sentiment)}
                resultHr.saveToCassandra("twitterseriesstreaming", "trendinghour", SomeColumns("year", "month", "day", "hour", "frequency", "ticker", "sentiment"))          
            case "MIN" =>
                val resultMin = result.map{case(date, frequency, ticker, sentiment)=> (date.split('-')(0).toInt, date.split('-')(1).toInt, date.split('-')(2).toInt, date.split('-')(3).toInt, date.split('-')(4).toInt, frequency, ticker, sentiment)}
                resultMin.saveToCassandra("twitterseriesstreaming", "trendingminute", SomeColumns( "year", "month", "day", "hour", "minute", "frequency", "ticker", "sentiment"))            
            //case "SEC" =>
            //result.map{case((a,ticker),b,c,d,e,f)=> (ticker, a.split('-')(0).toInt, a.split('-')(1).toInt, a.split('-')(2).toInt, a.split('-')(3).toInt, a.split('-')(4).toInt, a.split('-')(5).toInt, b, c, d, e, f)}
            //resultCassandra.saveToCassandra("stockdata", "daystock", SomeColumns("ticker", "year", "month", "day", "hour", "minute", "second", "high", "low", "open", "close", "volume"))                         
        }*/


    ssc.start()
    ssc.awaitTermination()


  }



    def main(args: Array[String]) 
    {

        /*if (args.length != 1) 
        {
            System.err.println("twitterGetCount requires one argument")
            System.exit(1)
        }*/

        getResult()
    }

  
}




