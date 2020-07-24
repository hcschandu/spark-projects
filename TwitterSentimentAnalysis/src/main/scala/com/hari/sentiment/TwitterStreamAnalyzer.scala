package com.hari.sentiment

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.util.CoreMap
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._

import collection.JavaConverters._

object TwitterStreamAnalyzer {

  def main(args: Array[String]): Unit = {

    // Inserting twitter key details generated from twitter developer application
    val consumerKey = "*****CONSUMER KEY*****"
    val consumerSecret = "*****CONSUMER SECRET*****"
    val accessToken = "*****ACCESS TOKEN*****"
    val accessTokenSecret = "*****ACCESS TOKEN SECRET*****"

    // Set System properties - twitter authentication details
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Spark context
    val sc = new SparkContext("local[*]" , "SparkDemo")

    // Turning off all Logs
    sc.setLogLevel("OFF")

    // Generating streamingContext
    val streamingContext = new StreamingContext(sc, Seconds(5));

    // Keywords to search in tweets
    val filters = Seq("Trump, India, Modi")

    // Applying filter
    val tweetStream = TwitterUtils.createStream(streamingContext, None, filters)

    // Check only for tweets in English
    val tweets = tweetStream.filter(_.getLang == "en")

    // Printout tweets with sentiment
    tweets.map(status => status.getText).map(tweet => (tweet, sentiment(tweet)))
      .foreachRDD(rdd => rdd.collect().foreach(tuple => println(" Sentiment => " + tuple._2 + " :-: TWEET => " + tuple._1)))

    // Start and terminate stream
    streamingContext.start();
    streamingContext.awaitTermination();
  }

  // Sentiment detection method
  def sentiment(tweets: String): String = {
    var mainSentiment = 0
    var longest = 0;
    val sentimentText = Array("Very Negative", "Negative", "Neutral", "Positive", "Very Positive")
    val props = new Properties();
    props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
    new StanfordCoreNLP(props).process(tweets).get(classOf[CoreAnnotations.SentencesAnnotation]).asScala.foreach((sentence: CoreMap) => {
      val sentiment = RNNCoreAnnotations.getPredictedClass(sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree]));
      val partText = sentence.toString();
      if (partText.length() > longest) {
        mainSentiment = sentiment;
        longest = partText.length();
      }
    })
    sentimentText(mainSentiment)
  }

}
