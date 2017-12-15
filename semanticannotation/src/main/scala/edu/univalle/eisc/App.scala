package edu.univalle.eisc

/**
 * @author ${user.name}
 */

import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._
//import com.ning.http.client
import com.sksamuel.elastic4s.searches.RichSearchResponse
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import com.sksamuel.elastic4s.{ElasticsearchClientUri, TcpClient}
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy

/*
// the query dsl
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._

// elasticsearch stuff
import org.elasticsearch.action.search.SearchResponse

// scala concurrency stuff
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import scala.util.{Success, Failure}


// elasticsearch stuff
import org.elasticsearch.action.search.SearchResponse
*/

//EJ2
//import jp.co.bizreach.elasticsearch4s._

//EJ3
/*
import wabisabi._
import org.elasticsearch.common.settings.Settings
import org.asynchttpclient._
import dispatch._
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy
*/

object HelloWorld {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    import com.sksamuel.elastic4s.ElasticDsl._


    val client = TcpClient.transport(ElasticsearchClientUri("192.168.0.108", 9300))

    val result = client.execute {
      search("myindex").matchQuery("capital", "ulaanbaatar")
    }.await

    // prints out the original json
    println(result.hits.head.sourceAsString)

    client.close()
    

    val input = Seq(
      (1, "Stanford University is located in California. It is a great university.")
    ).toDF("id", "text")

    val output = input
      .select(cleanxml('text).as('doc))
      .select(explode(ssplit('doc)).as('sen))
      .select('sen, tokenize('sen).as('words), ner('sen).as('nerTags), sentiment('sen).as('sentiment))

    output.show(truncate = false)
  }
}