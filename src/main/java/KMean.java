import model.Point;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.api.java.function.ForeachFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import static org.apache.spark.sql.functions.*;

public class KMean {

    private static final int K = 3;
    private static final int MAX_ITER = 100;

    private static HashMap<String, Double> initBoundaries() {
        HashMap<String, Double> boundaries = new HashMap<>();
        //TODO: read from file
        boundaries.put("x_min", 0.);
        boundaries.put("x_max", 10.);
        boundaries.put("y_min", 0.);
        boundaries.put("y_max", 10.);
        return boundaries;
    }

    private static ArrayList<Point> initCentroids(HashMap<String, Double> boundaries) {
        Random random = new Random();
        ArrayList<Point> centroids = new ArrayList<>();
        int i = 0;
        while (i < K) {
            double low = boundaries.get("x_min"), high = boundaries.get("x_max");
            double x = random.nextDouble() * (high - low) + low;
            low = boundaries.get("y_min");
            high = boundaries.get("y_max");
            double y = random.nextDouble() * (high - low) + low;
            centroids.add(new Point(x, y, i));
            i++;
        }
        return centroids;
    }

    private static Integer assignCentroid(Point p, ArrayList<Point> centroids) {
        assert !centroids.isEmpty();
        Point nearest = null;
        Double minDistance = null;
        for (Point centroid : centroids) {
            double distance = p.getDistance(centroid);
            if (minDistance == null || distance < minDistance) {
                minDistance = distance;
                nearest = centroid;
            }
        }
        return nearest.getId();
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("k-mean")
                .getOrCreate();
        SQLContext sqlContext = new SQLContext(spark.sparkContext());
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        ArrayList<Point> centroids = initCentroids(initBoundaries());
        Broadcast<ArrayList<Point>> broadcastCentroids = jsc.broadcast(centroids);

        sqlContext.udf().register("assignCentroid",
                (UDF2<Double, Double, Integer>) (x, y) -> assignCentroid(new Point(x, y), broadcastCentroids.getValue()),
                DataTypes.IntegerType);

        Dataset<Row> input = spark.read().option("header", "true").option("inferSchema", "true").csv("input"),
                df = null;

        for (int i = 0; i < MAX_ITER; i++) {
            df = input.withColumn("centroid", callUDF("assignCentroid", col("x"), col("y")))
                    .groupBy("centroid").avg("x", "y");
            df.foreach((ForeachFunction<Row>) row -> {
                Point p = new Point(row.getAs("avg(x)"), row.getAs("avg(y)"));
                p.setId(row.getAs("centroid"));
                broadcastCentroids.getValue().set(row.getAs("centroid"), p);
            });
        }
        df.toJavaRDD().saveAsTextFile("output");

    }
}
