package spark;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
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
		JavaPairRDD<String, Tuple2<Double, Set<String>>> RDD =  preprocessRevisions(sc.textFile(args[0]), args[2]);
		JavaPairRDD<String, Set<String>> outgoingLinksRDD = RDD.mapToPair(r -> {
			return new Tuple2<String, Set<String>>(r._1(), r._2()._2());
		});
		calculatePageRank(RDD, outgoingLinksRDD, 2)
			.mapValues(v -> v._1())
			.saveAsTextFile(args[1]);
		sc.close();
	}
	
	private static JavaPairRDD<String, Tuple2<Double, Set<String>>> preprocessRevisions(JavaRDD<String> rdd, String date) { 
		try {
			Long argumentTimestamp = ISO8601.toTimeMS(date);
			return rdd.mapToPair(s -> {
				String[] lines = s.split("\n");
				String[] revisionFields = lines[0].split(" ");
				Set<String> linksOut= new HashSet<String>();
				for (String outgoingLink : lines[3].split("\\s+")) {
					if (outgoingLink.equals("MAIN")) continue;
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
				return new Tuple2<String, Tuple2<Double, Set<String>>>(record._1(), new Tuple2<Double, Set<String>>(1.0, record._2()._2()));
			});
		} catch (ParseException e) {
			System.out.println("The specified date was not in correct format!");
			e.printStackTrace();
			System.exit(1);
		}
		return null;
		
	}
	
	private static JavaPairRDD<String, Tuple2<Double, Set<String>>> calculatePageRank(JavaPairRDD<String, Tuple2<Double, Set<String>>> rdd,
			JavaPairRDD<String, Set<String>> outgoingLinksRDD,
			int iterations) {
		
		JavaPairRDD<String, Tuple2<Double, Set<String>>> newPageRanks = rdd;
		for (int i = 0;i < iterations; i++) {
			JavaPairRDD<String, Double> pageRankRDD = newPageRanks.flatMapToPair(r -> {
				List<Tuple2<String, Double>> contributions = new ArrayList<Tuple2<String, Double>>();
				for (String outgoingLink: r._2()._2()) {
					Double contribution = r._2()._1()/r._2()._2().size();
					contributions.add(new Tuple2<String, Double>(outgoingLink, contribution));
				}
				return contributions;
			})
			.reduceByKey((cont1, cont2) -> cont1 + cont2)
			.mapValues(v -> 0.15 + 0.85 * v);
			newPageRanks = pageRankRDD.join(outgoingLinksRDD);
		}
		return newPageRanks;
	}
	
}
