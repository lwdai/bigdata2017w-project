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
import org.apache.spark.graphx._
import org.rogach.scallop._


class DegreeDistributionConfig(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object DegreeDistribution {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {

    val args = new DegreeDistributionConfig(argv)

    log.info("Input: " + args.input())

    val conf = new SparkConf().setAppName("Degree Distribution")
    val sc = new SparkContext(conf)

    //val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, args.input()).cache()

    val graph = CompressGraph.loadCompressedGraph(args.input(), sc).cache()

    val outputDir = new Path(args.output())
    val deleted = FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    if (deleted) println("output directory is deleted.")

    graph.inDegrees.map( vid_deg => ( vid_deg._2, 1) ).
      reduceByKey( _ + _ ).sortByKey(numPartitions = 1).map( deg_count => deg_count._1 + " " + deg_count._2).
      saveAsTextFile(args.output() + "/inDegrees")

    graph.outDegrees.map( vid_deg => ( vid_deg._2, 1) ).
      reduceByKey( _ + _ ).sortByKey(numPartitions = 1).map( deg_count => deg_count._1 + " " + deg_count._2).
      saveAsTextFile(args.output() + "/outDegrees")

    sc.stop()
  }
}
// scalastyle:on println
