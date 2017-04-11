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
import org.rogach.scallop._
import org.apache.spark.graphx._


class KBetweennessConfig(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val seeds = opt[Int](descr = "number of seeds (K)", required = true)
  val diameter = opt[Int](descr = "max number of iterations for one BFS", required = true)
  verify()
}

object KBetweenness {
  val log = Logger.getLogger(getClass().getName())

  def initBFS( graph: Graph[(Int, Long, Double), (Double, Double)], root: Long):
      Graph[(Int, Long, Double), (Double, Double)] = {

    graph.mapVertices[(Int, Long, Double)]((vid, v) =>
      ( if (vid == root) 0 else Integer.MAX_VALUE,
        if (vid == root) 1 else 0,
        1.0) ).mapEdges( e => (e.attr._1, 0.0)  )
  }

  // If receives a message s.t. isLeaf = false, set credit to 0.0
  def bfsVprog(vid: Long, vd: (Int, Long, Double), msg: (Int, Long, Boolean)): (Int, Long, Double) = {
    ( if (vd._1 == Integer.MAX_VALUE && msg._1 < Integer.MAX_VALUE) msg._1 + 1 else vd._1,
      vd._2 + msg._2,
      if (msg._3) vd._3 else 0.0 )
  }

  def bfsSendMsg(e: EdgeTriplet[(Int, Long, Double), (Double, Double)]): Iterator[(VertexId, (Int, Long, Boolean))] = {
    if ( e.srcAttr._1 < Integer.MAX_VALUE && e.dstAttr._1 == Integer.MAX_VALUE ) {
      // only send message if source vertex is visited and dst vertex is unvisited
      Iterator( (e.dstId, (e.srcAttr._1, e.srcAttr._2, true) ), ( e.srcId, (0, 0L, false) )  )
    } else {
      Iterator.empty
    }
  }

  def bfsMergeMsg(msg1: (Int, Long, Boolean), msg2: (Int, Long, Boolean)): (Int, Long, Boolean) = {
    ( if (msg1._1 < msg2._1) msg1._1 else msg2._1,
      if (msg1._1 < msg2._1) msg1._2
      else if (msg1._1 > msg2._1) msg2._2
      else msg1._2 + msg2._2,
      msg1._3 && msg2._3 )
  }

  def main(argv: Array[String]): Unit = {

    val args = new KBetweennessConfig(argv)

    log.info("Input: " + args.input())

    val conf = new SparkConf().setAppName("K-betweenness")
    val sc = new SparkContext(conf)

    //val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, args.input()).cache()

    // VD = (depth, number of shortest paths to root, credit); ED = (credit_acc, credit)
    //var graph: Graph[(Int, Long, Double), (Double, Double)] = CompressGraph.loadCompressedGraph(args.input(),
    //  sc)
    var graph: Graph[(Int, Long, Double), (Double, Double)] = GraphLoader.edgeListFile(sc, args.input())
      .mapVertices[(Int, Long, Double)]( (vid, v) => (0, 0, 0.0) ).
      mapEdges( e => (0.0, 0.0)).cache()

    val outputDir = new Path(args.output())
    val deleted = FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    if (deleted) println("output directory is deleted.")



    1 to args.seeds() foreach { _ => {
      //val root: Long = graph.pickRandomVertex()
      //println("Picked vertexID " + root + " as root for BFS.")
      val root = 1
      // message = (depth of parent, number of shortest paths to root, isLeaf)
      graph = initBFS(graph, root).pregel[(Int, Long, Boolean)]( (Integer.MAX_VALUE, 0, true), args.diameter(),
        EdgeDirection.Out)(
        bfsVprog, bfsSendMsg, bfsMergeMsg)
    } }

    // "vid depth #SP credit", where leaves have credit of 1
    graph.mapVertices( (vid, vd) => vid + " " + vd._1 + " " + vd._2 + " " + vd._3 ).vertices.saveAsTextFile(args
      .output())

    sc.stop()
  }
}
// scalastyle:on println
