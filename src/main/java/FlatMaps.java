import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlatMaps implements Serializable {

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

        sc.parallelize(inputData)
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .filter(word -> word.length() > 2)
                .collect().forEach(System.out::println);

        //código para filtrar a lista de String com o iterator de tamanho, filtrando palavras acima de tamanho 2, coletando e printando cada palavra.

//        JavaRDD<String> words = (sentences.flatMap(s -> Arrays.asList(s.split(" ")).iterator()));
//
//        JavaRDD<String> filter = words.filter(word -> word.length() > 2);
//
//        filter.collect().forEach(System.out::println);
        sc.close();



    }
}

