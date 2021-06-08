import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class YouTubeTagsCount {

	public static void main(String[] args) throws IOException{
		  Logger.getLogger ("org").setLevel (Level.ERROR);
		//Create a Spark conext
		SparkConf conf = new SparkConf().setAppName("Tags").setMaster("local[3]");
		JavaSparkContext context= new JavaSparkContext(conf);
		 // LOAD DATASETS
		JavaRDD<String> videos= context.textFile("E:\\iti\\Java & UML Programming\\DAY 7\\USvideos.csv");
		//Transformation
		JavaRDD<String> tags= videos
				.map(YouTubeTagsCount::extractTags)
				.filter(StringUtils::isNotBlank);
		//Tags manipulation
		JavaRDD<String> words= tags.flatMap(tag->Arrays.asList(tag
				.toLowerCase()
				.trim()
				.split("\\|")).iterator());
		System.out.println(words.toString());
		//counting
		Map<String,Long> wordCount= words.countByValue();
		List<Map.Entry> sorted= wordCount.entrySet().stream()
				.sorted(Map.Entry.comparingByValue()).collect(Collectors.toList());
		
		for (Map.Entry<String, Long> entry: sorted) System.out.println(entry.getKey () + " : " + entry.getValue ());
		}
	public static String extractTags(String videoLine) {
        try {
            return videoLine.split (",")[6];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
        
    }
	
}
