package edu.univalle.eisc

import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri, TcpClient}
import com.sksamuel.elastic4s.{ElasticsearchClientUri, TcpClient}

import org.elasticsearch.action.support.WriteRequest.RefreshPolicy
//import com.sksamuel.elastic4s.jackson.ElasticJackson.Implicits._


object getelastic extends App{
  import com.sksamuel.elastic4s.ElasticDsl._
  // Here we create an instance of the TCP client
  val client = TcpClient.transport(ElasticsearchClientUri("localhost", 9300))

  // await is a helper method to make this operation synchronous instead of async
  // You would normally avoid doing this in a real program as it will block your thread
  client.execute {
    indexInto("twitter" / "tweet").fields("user" -> "coldplay").refresh(RefreshPolicy.IMMEDIATE)
  }.await

  // now we can search for the document we just indexed
  val resp = client.execute {
    search("bands" / "artists").query("coldplay")
  }.await

  println(resp)

}
