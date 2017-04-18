package ca.uwaterloo.cs.bigdata2017w.project

import _root_.io.bespin.scala.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.rogach.scallop._


class TopKRootsConfig(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input)
  val input = opt[String](descr = "input path", required = true)
  val k = opt[Int](descr = "number of roots", required = false, default = Some(10))
  verify()
}

object TopKRoots {
  val log = Logger.getLogger(getClass().getName())


  def topKRoots( graph: Graph[Int, Int], k: Int ): Seq[Long] = {
    graph.outDegrees.map( v => (-v._2, v._1) ).takeOrdered(k).map( d => d._2 )
  }

  def main(argv: Array[String]): Unit = {

    val args = new TopKRootsConfig(argv)

    log.info("Input: " + args.input())

    val conf = new SparkConf().setAppName("BfsSummary")
    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, args.input() )

    val topNodes = topKRoots(graph, args.k())

    println( topNodes.toList )

    sc.stop()
  }
}