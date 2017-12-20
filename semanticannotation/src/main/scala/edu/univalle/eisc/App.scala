package edu.univalle.eisc

/**
 * @author ${user.name}
 */

import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object HelloWorld {
  def main(args: Array[String]): Unit = {

    val t0 = System.currentTimeMillis()

    val conf = new SparkConf().setAppName("Simple Application").setMaster("spark://registro:7077")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    for( a <- 1 to 1) {

      val input = Seq(
        (1, "The House, forced to vote a second time on the $1.5 trillion tax bill, moved swiftly to pass the final version on Wednesday, clearing the way for President Trump to sign into law the most sweeping tax overhaul in decades.House lawmakers approved the tax bill 224 to 201 on Wednesday, after being forced to vote on the bill again after last-minute revisions were made to the Senate bill, which passed that chamber 51 to 48 early Wednesday morning.\n\nThe final House vote was essentially a formality, as the changes, which were made to comply with Senate budget rules, did not significantly alter the overall bill. But the need for a second vote gave ammunition to Democrats, who had already accused Republicans of trying to rush the tax overhaul through the House and Senate.We are five days away from Christmas, but it feels like Groundhog Day, said Representative Louise M. Slaughter, Democrat of New York, who denounced the process by which Republicans undertook their tax rewrite as “nothing short of an abomination."),
        (2, "The overhaul drops the corporate rate to 21 percent, from 35 percent, as Republicans seek to boost American competitiveness and spur economic growth. In addition, it provides a tax break to owners of pass-through businesses, whose profits are taxed through the individual code.\n\nThe bill also cuts taxes for individuals, including a lower top rate of 37 percent, down from 39.6 percent. It nearly doubles the standard deduction and doubles the child tax credit, and it also doubles the size of inheritances shielded from estate taxation, to $22 million for married couples. But Republicans set the individual tax cuts to expire after 2025 in order to make the bill comply with budget rules."),
        (3, "In a move that drew significant criticism from lawmakers from states with high taxes, the bill caps the deduction for state and local taxes at $10,000. Twelve House Republicans voted against the tax bill on both Tuesday and Wednesday, and 11 of those members were from California, New Jersey and New York, three states with high taxes.\n\nThe bill also eliminates the Affordable Care Act’s requirement that most people have health insurance or pay a penalty, known as the individual mandate, providing Republicans with a victory on health care after their previous failure to repeal and replace President Barack Obama’s health law. It also opens the Arctic National Wildlife Refuge in Alaska to oil and gas drilling.")
      ).toDF("id", "text")



      val output = input
        .select(cleanxml('text).as('doc))
        .select(explode(ssplit('doc)).as('sen))
        .select('sen, tokenize('sen).as('words), ner('sen).as('nerTags), sentiment('sen).as('sentiment))

      output.show(truncate = true)
    }
      val t1 = System.currentTimeMillis()

    println("Elapsed time: " + (t1 - t0) + "s")

  }
}