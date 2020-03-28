package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Exercise_3 {

    private static class VProg extends AbstractFunction3<Long, Tuple2<Integer, ArrayList<Object>>, Tuple2<Integer, ArrayList<Object>>, Tuple2<Integer, ArrayList<Object>>> implements Serializable {
        @Override
        public Tuple2<Integer, ArrayList<Object>> apply(Long vertexID, Tuple2<Integer, ArrayList<Object>> vertexValue, Tuple2<Integer, ArrayList<Object>> message) {
            if (message._1 == Integer.MAX_VALUE || message._1 < 0) {             // superstep 0
                return vertexValue;

            } else {
                // superstep > 0
                Integer cost = vertexValue._1;
                ArrayList<Object> path = vertexValue._2;

                if(message._1 < vertexValue._1){ //if cost is less then update cost and
                    cost = message._1;
                    path = message._2;
                    path.add(vertexID);
                }
                Tuple2<Integer, ArrayList<Object>> msg = new Tuple2<Integer, ArrayList<Object>>(cost, path);

                return msg;

            }
        }
    }

    private static class sendMsg extends AbstractFunction1< EdgeTriplet<Tuple2<Integer, ArrayList<Object>>, Integer>,
            Iterator<Tuple2<Object, Tuple2<Integer, ArrayList<Object>>>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Tuple2<Integer, ArrayList<Object>>>> apply(EdgeTriplet<Tuple2<Integer, ArrayList<Object>>, Integer> triplet) {


            Tuple2<Object, Tuple2<Integer, ArrayList<Object>>> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object, Tuple2<Integer, ArrayList<Object>>> dstVertex = triplet.toTuple()._2();

            Integer pathCost = sourceVertex._2._1 + triplet.toTuple()._3();

            if (pathCost >= dstVertex._2._1 || sourceVertex._2._1 == Integer.MAX_VALUE) {   // source vertex value is smaller than dst vertex?
                // do nothing
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object, Tuple2<Integer, ArrayList<Object>>>>().iterator()).asScala();
            } else {
                // propagate source vertex value
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object, Tuple2<Integer, ArrayList<Object>>>(triplet.dstId(), new Tuple2<>(pathCost, triplet.srcAttr()._2))).iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Tuple2<Integer, ArrayList<Object>>,Tuple2<Integer, ArrayList<Object>>,Tuple2<Integer, ArrayList<Object>>> implements Serializable {
        @Override
        public Tuple2<Integer, ArrayList<Object>> apply(Tuple2<Integer, ArrayList<Object>> msg1, Tuple2<Integer, ArrayList<Object>> msg2) {
            if(msg1._1 <= msg2._1)
                return msg1;
            else
                return msg2;
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();

        List<Tuple2<Object, Tuple2<Integer, ArrayList<Object>>>> vertices = Lists.newArrayList(

                new Tuple2<Object, Tuple2<Integer, ArrayList<Object>>>(1l, new Tuple2<Integer, ArrayList<Object>>(0, new ArrayList<Object>(Arrays.asList(1l)))),
                new Tuple2<Object, Tuple2<Integer, ArrayList<Object>>>(2l, new Tuple2<Integer, ArrayList<Object>>(Integer.MAX_VALUE, new ArrayList<Object>(Arrays.asList(2l)))),
                new Tuple2<Object, Tuple2<Integer, ArrayList<Object>>>(3l, new Tuple2<Integer, ArrayList<Object>>(Integer.MAX_VALUE, new ArrayList<Object>(Arrays.asList(3l)))),
                new Tuple2<Object, Tuple2<Integer, ArrayList<Object>>>(4l, new Tuple2<Integer, ArrayList<Object>>(Integer.MAX_VALUE, new ArrayList<Object>(Arrays.asList(4l)))),
                new Tuple2<Object, Tuple2<Integer, ArrayList<Object>>>(5l, new Tuple2<Integer, ArrayList<Object>>(Integer.MAX_VALUE, new ArrayList<Object>(Arrays.asList(5l)))),
                new Tuple2<Object, Tuple2<Integer, ArrayList<Object>>>(6l, new Tuple2<Integer, ArrayList<Object>>(Integer.MAX_VALUE, new ArrayList<Object>(Arrays.asList(6l))))
        );

        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l, 2l, 4), // A --> B (4)
                new Edge<Integer>(1l, 3l, 2), // A --> C (2)
                new Edge<Integer>(2l, 3l, 5), // B --> C (5)
                new Edge<Integer>(2l, 4l, 10), // B --> D (10)
                new Edge<Integer>(3l, 5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)

        );

        JavaRDD<Tuple2<Object, Tuple2<Integer, ArrayList<Object>>>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Tuple2<Integer, ArrayList<Object>>, Integer> G = Graph.apply(verticesRDD.rdd(), edgesRDD.rdd(), new Tuple2<Integer, ArrayList<Object>>(Integer.MAX_VALUE, new ArrayList<Object>()) ,StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class), scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class), scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        ops.pregel(new Tuple2<Integer, ArrayList<Object>>(Integer.MAX_VALUE, new ArrayList<Object>()),
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new VProg(),
                new sendMsg(),
                new merge(),
                ClassTag$.MODULE$.apply(Tuple2.class))
                .vertices()
                .toJavaRDD().sortBy(f -> ((Tuple2<Object, Tuple2<Integer, ArrayList<Object>>>) f)._1, true, 0)
                .foreach(v -> {
                    Tuple2<Object, Tuple2<Integer, ArrayList<Integer>>> vertex = (Tuple2<Object, Tuple2<Integer, ArrayList<Integer>>>) v;
                    System.out.print("Minimum path to get from " + labels.get(1l) + " to " + labels.get(vertex._1) + " is [");
                    int pathLength = vertex._2._2.size();
                    for(int i = 0; i < pathLength; i++){
                        System.out.print(labels.get(vertex._2._2.get(i)));
                        if(i != pathLength-1)
                            System.out.print(",");
                    }
                    System.out.print("] with cost " + vertex._2._1+"\n");
                });
        }

}
