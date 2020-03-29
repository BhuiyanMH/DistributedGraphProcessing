package exercise_4;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class Exercise_4 {

	static String VERTEX_FILE_PATH = "src/main/resources/wiki-vertices.txt";
	static String EDGE_FILE_PATH = "src/main/resources/wiki-edges.txt";

	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {

		Dataset<Row> vertices = getVertexFrames(ctx, sqlCtx);
		Dataset<Row> edges = getEdgeFrames(ctx, sqlCtx);

		GraphFrame wikiGraphFrame = GraphFrame.apply(vertices,edges);

		PageRank pageRank = new PageRank(wikiGraphFrame);
		pageRank.resetProbability(0.15);
		pageRank.maxIter(5);

		GraphFrame resultGraphFrame = pageRank.run();
		//resultGraphFrame.vertices().show();

		System.out.println("Top 10 most relevant articles:");
		Dataset<Row> sortedVertex = resultGraphFrame.vertices().orderBy(col("pagerank").desc());
		sortedVertex.show(10);
	}

	private static Dataset<Row> getVertexFrames(JavaSparkContext ctx, SQLContext sqlCtx){

		try {

			File file = new File(VERTEX_FILE_PATH);
			BufferedReader bufferedReader = new BufferedReader(new FileReader(file));

			String row;

			java.util.List<Row> vertices_list = new ArrayList<Row>();
			while ((row = bufferedReader.readLine()) != null){
				String[] columns = row.split("\\t", -1);
				//System.out.println("ID: "+columns[0]+" Name: "+ columns[1]);
				vertices_list.add(RowFactory.create(columns[0], columns[1]));
			}

			JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);

			StructType vertices_schema = new StructType(new StructField[]{

					new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
					new StructField("title", DataTypes.StringType, true, new MetadataBuilder().build())
			});

			Dataset<Row> vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);
			return vertices;

		} catch (FileNotFoundException e) {
			System.out.println("Vertex File Not Found");
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	private static Dataset<Row>  getEdgeFrames(JavaSparkContext ctx, SQLContext sqlCtx){

		try {

			File file = new File(EDGE_FILE_PATH);
			BufferedReader bufferedReader = new BufferedReader(new FileReader(file));

			String row;

			java.util.List<Row> edge_list = new ArrayList<Row>();
			while ((row = bufferedReader.readLine()) != null){
				String[] columns = row.split("\\t", -1);
				//System.out.println("ID: "+columns[0]+" Name: "+ columns[1]);
				edge_list.add(RowFactory.create(columns[0], columns[1]));
			}

			JavaRDD<Row> edges_rdd = ctx.parallelize(edge_list);

			StructType edge_schema = new StructType(new StructField[]{
					new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
					new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build())
			});

			Dataset<Row> edges =  sqlCtx.createDataFrame(edges_rdd, edge_schema);
			return edges;

		} catch (FileNotFoundException e) {
			System.out.println("Edge File Not Found");
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

	}
}
