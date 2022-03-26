

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class Main implements Serializable {

    public static void main(String[] args) throws IOException {
        List<String> inputData = new ArrayList<>();

        System.setProperty("hadoop.home.dir", "c:/hadoop");
        inputData.add("WARN: Terça-Feira 12 de Abril de 2022");
        inputData.add("ERROR: Quarta-Feira 16 de Maio de 2022");
        inputData.add("WARN: Terça-Feira 25 de Abril de 2022");
        inputData.add("ERROR: Quinta-Feira 12 de Junho de 2022");
        inputData.add("FATAL: Sexta-Feira 12 de Maio de 2022");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> originalLogValues = sc.parallelize(inputData);

        JavaPairRDD<String, Long> pairRDD = (originalLogValues.mapToPair(s -> {
           String[] splitValues = s.split(":");
           String levelLog = splitValues[0];
           return new Tuple2<>(levelLog, 1L);

        }));

        JavaPairRDD<String, Long> stringLongJavaPairRDD = pairRDD.reduceByKey((value1, value2) -> value1 + value2);
        stringLongJavaPairRDD.foreach((tuple -> System.out.println(tuple._1 + "tem" + tuple._2 + "instancias")));

        sc.close();

    }
}
