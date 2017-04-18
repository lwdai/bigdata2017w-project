package ca.uwaterloo.cs.bigdata2017w.project

import _root_.io.bespin.scala.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.rogach.scallop._


class BfsSummaryConfig(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input)
  val input = opt[String](descr = "input path, bfs result", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object BfsSummary {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {

    val args = new BfsSummaryConfig(argv)

    log.info("Input: " + args.input())

    val conf = new SparkConf().setAppName("BfsSummary")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    val deleted = FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    if (deleted) println("output directory is deleted.")

    val lines: RDD[(Int, Long)] = sc.textFile(args.input()).map( line => {
      val tokens = line.split(" ")
      val depth = tokens(1).toInt
      val nsp = tokens(2).toLong

      (depth, nsp)
    })

    lines.map( x => (x._1, 1) ).reduceByKey( _ + _, 1).sortByKey().map( x => x._1 + " " + x._2 ).saveAsTextFile(args
      .output
      () +
      "/depth")

    lines.map( x => (x._2, 1) ).reduceByKey( _ + _, 1).sortByKey().map( x => x._1 + " " + x._2 ).saveAsTextFile(args
      .output() +
      "/nsp")

    sc.stop()
  }
}