/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs.bigdata2017w.project

import _root_.io.bespin.scala.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.rogach.scallop._
import org.apache.spark.graphx._


class DetectCommunityConfig(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val stat = opt[String](descr = "stat output path", required = true)
  //val betweenness= opt[Int](descr = "betweenness output", required = true)
  val iter = opt[Int](descr = "max number of iterations", required = false, default = Some(2))
  val threshold = opt[Double](descr = "betweenness threshold", required = true)
  verify()
}

object DetectCommunity {
  val log = Logger.getLogger(getClass().getName())


  def main(argv: Array[String]): Unit = {

    val args = new DetectCommunityConfig(argv)

    log.info("Input: " + args.input())

    val conf = new SparkConf().setAppName("DetectCommunity")
    val sc = new SparkContext(conf)

    //var graph: Graph[(Int, Long, Double), (Double, Double)] = CompressGraph.loadCompressedGraph(args.input(),
    //  sc)

    val edges: RDD[Edge[Double]] = sc.textFile(args.input()).flatMap( line => {
      val tokens = line.split(" ")
      if (tokens.size < 3) {
        List()
      } else {
        val src = tokens(0).toInt
        val dst = tokens(1).toLong
        val bet = tokens(2).toDouble
        List( new Edge[Double](src, dst, bet) )
      }
    } )

    val threshold = args.threshold()

    val originalGraph: Graph[Int, Double] = Graph.fromEdges[Int, Double](edges, 0).cache()

    val path = args.output()
    val outputDir = new Path(path)
    val deleted = FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val workGraph: Graph[Int, Double] = Graph(originalGraph.vertices, originalGraph.edges.filter( e => e.attr <
      threshold)).cache()

    originalGraph.unpersist(false)
    val scc = workGraph.stronglyConnectedComponents(args.iter()).cache()


    scc.vertices.map( v => v._1 + " " + v._2 ).repartition(1).saveAsTextFile(args.output())


    val path2 = args.stat()
    val outputDir2 = new Path(path2)
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir2, true)

    scc.vertices.map( v => (v._2, 1)).reduceByKey(_ + _, 1).saveAsTextFile(args.stat())


    sc.stop()
  }
}
// scalastyle:on println
