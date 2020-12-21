package utils;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class SparkUtils {

    public static SparkSession sparkSesion = SparkSession
            .builder()
            .appName("ImdbProcessor")
            .master("local[4]")
            .getOrCreate();

    public static SQLContext sparkContext = new SQLContext(sparkSesion);

}
