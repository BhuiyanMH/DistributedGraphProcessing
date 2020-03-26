package exercise_1;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Exercise_1 {

    private static class VProg extends AbstractFunction3<Long, Integer, Integer, Integer> implements Serializable {
        @Override
        public Integer apply(Long vertexID, Integer vertexValue, Integer message) {
            if (message == Integer.MAX_VALUE) {             // superstep 0
                return vertexValue;
            } else {                                        // superstep > 0
                return Math.max(vertexValue, message);
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Integer, Integer>, Iterator<Tuple2<Object, Integer>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Integer>> apply(EdgeTriplet<Integer, Integer> triplet) {
            Tuple2<Object, Integer> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object, Integer> dstVertex = triplet.toTuple()._2();

            if (sourceVertex._2 <= dstVertex._2) {   // source vertex value is smaller than dst vertex?
                // do nothing
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object, Integer>>().iterator()).asScala();
            } else {
                // propagate source vertex value
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object, Integer>(triplet.dstId(), sourceVertex._2)).iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Integer, Integer, Integer> implements Serializable {
        @Override
        public Integer apply(Integer o, Integer o2) {
            return Math.max(o, o2);
        }
    }

    public static void maxValue(JavaSparkContext ctx) { //This method is called from the main method
        List<Tuple2<Object, Integer>> vertices = Lists.newArrayList( //creating the graph
                new Tuple2<Object, Integer>(1l, 9),
                new Tuple2<Object, Integer>(2l, 1),
                new Tuple2<Object, Integer>(3l, 6),
                new Tuple2<Object, Integer>(4l, 8)
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l, 2l, 1), //edge between node 1 and 2, all edges are of uniform weight
                new Edge<Integer>(2l, 3l, 1),
                new Edge<Integer>(2l, 4l, 1),
                new Edge<Integer>(3l, 4l, 1),
                new Edge<Integer>(3l, 1l, 1)
        );

        //collection of vertices and edges are parallelized to form a distributed dataset that can be operated on in parallel
        //RDD: Resilient Distributed Datasets (RDD) is a fundamental data structure of Spark
        JavaRDD<Tuple2<Object, Integer>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        //create graph from the vertex and edge RDDs
        Graph<Integer, Integer> G = Graph.apply(verticesRDD.rdd(), edgesRDD.rdd(), 1, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class), scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Integer.class), scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        Tuple2<Long, Integer> max = (Tuple2<Long, Integer>) ops.pregel(
                Integer.MAX_VALUE,  //initial message
                Integer.MAX_VALUE,      // maximum iteration, run until convergence
                EdgeDirection.Out(),    //edge direction
                new VProg(),            //apply: vertex program
                new sendMsg(),          //scatter: send message
                new merge(),            //gather: merge
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class))
                .vertices().toJavaRDD().first();

        System.out.println(max._2 + " is the maximum value in the graph");
    }

}
