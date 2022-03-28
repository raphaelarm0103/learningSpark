
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;


public class ReadFromDisk implements Serializable {

    public static void main(String[] args) throws IOException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        System.setProperty("hadoop.home.dir", "c:/hadoop");

        SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> inputsText = sc.textFile("src/main/resources/inputs.txt");


        JavaRDD<String> lettersOnly = inputsText.map(s -> s.replaceAll("[^A-Za-z\\s]", "").toLowerCase());
        JavaRDD<String> removeBlankLines = lettersOnly.filter(s -> s.trim().length() > 0);
        JavaRDD<String> justWords = removeBlankLines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        JavaPairRDD<String, Long> stringLongJavaPairRDD = justWords.mapToPair(s -> new Tuple2<String, Long>(s, 1L));
        JavaPairRDD<String, Long> totals = stringLongJavaPairRDD.reduceByKey(Long::sum);

        JavaPairRDD<Long, String> map = totals.mapToPair(s -> new Tuple2<Long, String>(s._2, s._1));
        JavaPairRDD<Long, String> sorted = map.sortByKey(false);
        List<Tuple2<Long, String>> take = sorted.take(50);

        take.forEach(System.out::println);

        sc.close();
    }
}
