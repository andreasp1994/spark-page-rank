package spark;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import utils.ISO8601;

public class PageRank {
	
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("WordCount-v0").setMaster("local"));
		sc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n");
		JavaPairRDD<String, Set<String>> links =  preprocessRevisions(sc.textFile(args[0]), args[3]);
		calculatePageRank(links, Integer.valueOf(args[2]))
			.map(r -> r._1 + "\t" + r._2)
			.saveAsTextFile(args[1]);
		sc.close();
	}
	
	private static JavaPairRDD<String, Set<String>> preprocessRevisions(JavaRDD<String> rdd, String date) { 
		try {
			Long argumentTimestamp = ISO8601.toTimeMS(date);
			return rdd.mapToPair(s -> {
				String[] lines = s.split("\n");
				String[] revisionFields = lines[0].split(" ");
				Set<String> linksOut= new HashSet<String>();
				for (String outgoingLink : lines[3].split("\\s+")) {
					if (outgoingLink.equals("MAIN") || outgoingLink.equals(revisionFields[3])) continue;
					linksOut.add(outgoingLink);
				}
				return new Tuple2<String, Tuple2<String, Set<String>>>(revisionFields[3], new Tuple2<String, Set<String>>(revisionFields[4], linksOut));
			})
			.filter(revision -> {
				Long revisionTimestamp = ISO8601.toTimeMS(revision._2()._1());
				return revisionTimestamp <= argumentTimestamp;
			})
			.reduceByKey((v1, v2)-> {
				Long previousRevisionTimestamp = ISO8601.toTimeMS(v1._1());
				Long revisionTimestamp = ISO8601.toTimeMS(v2._1());
				if (revisionTimestamp > previousRevisionTimestamp && revisionTimestamp <= argumentTimestamp) return v2;
				else return v1;
			})
			.mapToPair((record) -> {
				return new Tuple2<String, Set<String>>(record._1(), record._2()._2());
			});
		} catch (ParseException e) {
			System.out.println("The specified date was not in correct format!");
			e.printStackTrace();
			System.exit(1);
		}
		return null;
		
	}
	
	private static JavaPairRDD<String, Double> calculatePageRank(
			JavaPairRDD<String, Set<String>> links,
			int iterations) {
		// Initialize Ranks
		JavaPairRDD<String, Double> ranks = links.mapValues( v -> 1.0);
		for (int i = 0;i < iterations; i++) {
			JavaPairRDD<String, Double> contributionsRdd = links.join(ranks).flatMapToPair(r -> {
				List<Tuple2<String, Double>> contributions = new ArrayList<Tuple2<String, Double>>();
				contributions.add(new Tuple2<String, Double>(r._1, 0.0));
				for(String outLink: r._2()._1()) {
					Double contribution = r._2._2/r._2._1.size();
					contributions.add(new Tuple2<String, Double>(outLink, contribution));
				}
				return contributions;
			});
			ranks = contributionsRdd.reduceByKey((c1, c2) -> c1 + c2)
					.mapValues(v -> 0.15 + 0.85 * v);
		}
		return ranks;
	}
}
