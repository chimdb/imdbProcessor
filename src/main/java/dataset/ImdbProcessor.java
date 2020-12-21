package dataset;

import org.apache.spark.sql.*;
import utils.*;
import static org.apache.spark.sql.functions.*;

public class ImdbProcessor {

    public static void main(String[] args) {

        if (args.length != 1){
            System.out.println("==== One argument required: the local path! Exiting ====");
            System.exit(1);
        }

        String datasetPath = args[0];

        SparkSession sparkSession = SparkUtils.sparkSesion;
        SQLContext sparkContext = SparkUtils.sparkContext;

        Dataset<Row> ratingsDataset = Operations.loadDataset(
                sparkContext,
                datasetPath,
                Configurations.TITLE_RATINGS_DATASET);

        Dataset<Row> alternativeTitles = Operations.loadDataset(sparkContext,
                datasetPath,
                Configurations.TITLE_ALTERNATIVE_DATASET);

        long averageVote = Operations.computeAverageRatings(ratingsDataset);

        // Add a the new ranking column to the ratings dataset
        Dataset<Row> computeRankings = ratingsDataset.withColumn(
                "rank",
                col("numVotes")
                        .multiply(averageVote)
                        .divide(col("averageRating")));

        Dataset<Row> filterTopNMovies = computeRankings.filter(col("rank")
                .$greater(Configurations.RATING_THRESHOLD))
                .orderBy(desc("rank"))
                .limit(Configurations.TOP_N_RECORDS);

        // get alternative names for the top N titles

        Dataset<Row> ratingsAlternativesJoined = filterTopNMovies.join(alternativeTitles,
                ratingsDataset.col("tconst")
                        .equalTo(alternativeTitles.col("titleId")));

        ratingsAlternativesJoined.show();
        ratingsAlternativesJoined.select("title","tconst")
                .orderBy("tconst")
                .show();
        Dataset<Row> keepOriginalTitle = ratingsAlternativesJoined.filter(col("isOriginalTitle").equalTo(1))
                .withColumnRenamed("tconst", "pktconst");

        Operations.printAlternativeTitlesResult(ratingsAlternativesJoined);

        Dataset<Row> principals = Operations.loadDataset(
                sparkContext,
                datasetPath ,
                Configurations.PRINCIPALS_DATASET);

        Dataset<Row> filteredMovies = filterTopNMovies.withColumnRenamed("tconst", "l_tconst");

        Dataset<Row> ratingsPrincipalsJoined = filteredMovies
                .join(principals,
                        filteredMovies.col("l_tconst")
                        .equalTo(principals.col("tconst")))
                .drop("l_tconst");

        Dataset<Row> names = Operations.loadDataset(
                sparkContext,
                datasetPath,
                Configurations.NAMES_BASIC_DATASET);

        Dataset<Row> namesPerTitle = ratingsPrincipalsJoined.join(
                names,
                ratingsPrincipalsJoined.col("nconst")
                        .equalTo(names.col("nconst")));

        Dataset<Row> namesPerTitleOriginal = namesPerTitle.join(
                keepOriginalTitle,
                namesPerTitle.col("tconst")
                        .equalTo(keepOriginalTitle.col("pktconst")), "right");

        Operations.printActorsPerTitlesResult(namesPerTitleOriginal);

        sparkSession.stop();


    }

}
