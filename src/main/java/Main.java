

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class Main implements Serializable {

    public static void main(String[] args) throws IOException {
        List<Integer> inputData = new ArrayList<>();

        System.setProperty("hadoop.home.dir", "c:/hadoop");
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        Logger.getLogger("org.apa   che").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> myRdd = sc.parallelize(inputData);
        Integer result = myRdd.reduce(Integer::sum);

        JavaRDD<Double> sqrtRdd = myRdd.map(Math::sqrt);

        sqrtRdd.foreach(integer ->System.out.println(integer));
        System.out.println(result);

        sc.close();

    }
}
