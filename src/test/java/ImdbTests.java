import dataset.Operations;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.*;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.junit.Assert.*;

public class ImdbTests {

    public static SparkSession sparkSesion;
    public static SQLContext sparkContext;

    public static Dataset<Row> ratings = null;

    @BeforeClass
    public static void setupTests() {
        sparkSesion = SparkSession
                .builder()
                .appName("ImdbProcessor")
                .master("local[4]")
                .getOrCreate();

        sparkContext = new SQLContext(sparkSesion);

        ratings = Operations.loadDataset(
                sparkContext,
                System.getProperty("user.dir").concat("/src/main/resources"),
                "ratings.tsv");
    }

    @AfterClass
    public static void endTests() {
        sparkSesion.stop();
    }

    @Test
    public void testComputeAverageRating() {
        long expectedResult = 6;
        long actualResult = Operations.computeAverageRatings(ratings);
        assertEquals(expectedResult, actualResult);
    }

    @Test
    public void testFilterTopNMovies() {
        long averageRatings = Operations.computeAverageRatings(ratings);
        Dataset<Row> result = Operations.computeRankings(ratings, averageRatings);
        boolean hasColumn = Arrays.asList(result.columns()).contains("rank");

        assertEquals("tt0000001",
                result.select(col("tconst"))
                    .first()
                    .getString(0));

        assertTrue(hasColumn);
    }

}