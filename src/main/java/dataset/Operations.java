package dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.File;

import static org.apache.spark.sql.functions.sum;

public class Operations {
    public static Dataset<Row> loadDataset(SQLContext context, String localPath, String datasetName) {
        return context.read()
                .format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("inferSchema", "true")
                .option("header", "true")
                .option("delimiter", "\t")
                .load(localPath
                        .concat(File.separator)
                        .concat(datasetName));
    }

    public static long computeAverageRatings(Dataset<Row> rankingsDataset) {

        long totalLinesCount = rankingsDataset.count();
        long ratingsSum = rankingsDataset.agg(
                sum("averageRating").cast("long"))
                .first().getLong(0);

        return ratingsSum / totalLinesCount;
    }

    public static void printAlternativeTitlesResult(Dataset<Row> akaNamesDataset) {

        akaNamesDataset.select("title", "tconst")
                .orderBy("tconst")
                .coalesce(1).write().csv("aka_names.csv");
    }


    public static void printActorsPerTitlesResult(Dataset<Row> actorsDataset) {

        actorsDataset.select("title", "primaryName")
                .orderBy("title")
                .coalesce(1)
                .write()
                .csv("actors.csv");

    }
}
