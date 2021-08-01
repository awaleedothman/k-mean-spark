import model.Point;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

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

    private static Point addPoints(Point p1, Point p2) {
        Point point = new Point(p1.getX() + p2.getX(), p1.getY() + p2.getY());
        point.setCount(p1.getCount() + p2.getCount());
        return point;
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("k-mean");
        JavaSparkContext sc = new JavaSparkContext(conf);

        ArrayList<Point> centroids = initCentroids(initBoundaries());
        final JavaRDD<Point> input = sc.textFile("input").map(Point::new);
        JavaPairRDD<Integer, Point> rdd = null;
        final Broadcast<ArrayList<Point>> broadCast_Centroids = sc.broadcast(centroids);
        for (int i = 0; i < MAX_ITER; i++) {
            rdd = input
                    .mapToPair(p -> new Tuple2<>(assignCentroid(p, broadCast_Centroids.getValue()), p))
                    .reduceByKey(KMean::addPoints)
                    .mapValues(Point::normalize);

            Map<Integer, Point> map = rdd.collectAsMap();
            for (int id : map.keySet()) {
                Point p = map.get(id);
                p.setId(id);
                centroids.set(id, p);
            }
        }

        rdd.saveAsTextFile("output");

    }
}
