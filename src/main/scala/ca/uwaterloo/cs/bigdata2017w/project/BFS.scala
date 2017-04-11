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


class BFSConfig(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val root = opt[Int](descr = "source vertex id", required = true)
  val diameter = opt[Int](descr = "max number of iterations for one BFS", required = true)
  verify()
}

object BFS {
  val log = Logger.getLogger(getClass().getName())

  // VD = (depth, #SP, credit, tmp_credit); ED = credit
  def initBFS( graph: Graph[(Int, Long), Int], root: Long): Graph[(Int, Long), Int] = {

    graph.mapVertices[(Int, Long)]((vid, v) =>
      ( if (vid == root) 0 else Integer.MAX_VALUE,
        if (vid == root) 1 else 0 ) ).cache()
  }

  // message = (depth of parent, number of shortest paths to root)
  def bfsVprog(vid: Long, vd: (Int, Long), msg: (Int, Long)): (Int, Long) = {
    ( if (vd._1 == Integer.MAX_VALUE && msg._1 < Integer.MAX_VALUE) msg._1 + 1 else vd._1,
      vd._2 + msg._2 )
  }

  def bfsSendMsg(e: EdgeTriplet[(Int, Long), Int]): Iterator[(VertexId, (Int, Long))] = {
    if ( e.srcAttr._1 < Integer.MAX_VALUE && e.dstAttr._1 == Integer.MAX_VALUE ) {
      // only send message if source vertex is visited and dst vertex is unvisited
      Iterator( (e.dstId, (e.srcAttr._1, e.srcAttr._2) ) )
    } else {
      Iterator.empty
    }
  }

  def bfsMergeMsg(msg1: (Int, Long), msg2: (Int, Long)): (Int, Long) = {
    ( // depth of parent
      if (msg1._1 < msg2._1) msg1._1 else msg2._1,
      // # SP
      if (msg1._1 < msg2._1) msg1._2
      else if (msg1._1 > msg2._1) msg2._2
      else msg1._2 + msg2._2)
  }


  def main(argv: Array[String]): Unit = {

    val args = new BFSConfig(argv)

    log.info("Input: " + args.input())

    val conf = new SparkConf().setAppName("BFS")
    val sc = new SparkContext(conf)

    //var graph: Graph[(Int, Long, Double), (Double, Double)] = CompressGraph.loadCompressedGraph(args.input(),
    //  sc)
    val graph: Graph[(Int, Long), Int] = GraphLoader.edgeListFile(sc, args.input())
      .mapVertices[(Int, Long)]( (vid, v) => (0, 0) ).cache()



    val path = args.output()
    val outputDir = new Path(path)
    val deleted = FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    initBFS(graph, args.root()).
      pregel[(Int, Long)]((Integer.MAX_VALUE, 0), args.diameter() + 1, EdgeDirection.Out)(
        bfsVprog, bfsSendMsg, bfsMergeMsg ).
      vertices.map( v => v._1 + " " + v._2._1 + " " + v._2._2 ).
      saveAsTextFile(path)

    sc.stop()
  }
}
// scalastyle:on println
