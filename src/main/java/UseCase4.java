import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import java.io.File;
import static org.apache.spark.sql.functions.*;

public class UseCase4 {
    static final Logger logger=Logger.getLogger(UseCase4.class);
    public static boolean checkFileExist(String path)
    {
        try {
            File file = new File(path);
            return file.exists();
        }
        catch(Exception ex)
        {
            logger.error("ERROR=> FILE NOT PRESENT IN FUNCTION "+ ex.getMessage());
        }
        return false;
    }
    public static void main(String[] args) {
        logger.info("****************************************RUNNING USECASE4**********************************\n\n\n");
        String orderPath = System.getenv("ORDER_PATH");
        String productPath = System.getenv("PRODUCT_PATH");
        String orderItemsPath = System.getenv("ORDER_ITEM_PATH");
        String categoryPath = System.getenv("CATEGORY_PATH");
        if(UseCase4.checkFileExist(orderPath) && UseCase4.checkFileExist(productPath)&& UseCase4.checkFileExist(orderItemsPath)&& UseCase4.checkFileExist(categoryPath)) {
            logger.info("\n\n************************FILE EXISTS***************************\n\n");
            try {
                SparkSession spark = SparkSession.builder().master("local").getOrCreate();
                logger.info("\n\n************************SPARK SESSION CREATED***************************\n\n");

                Dataset<Row> orders = spark.read().format("csv").option("header", true).option("inferSchema", true).load(orderPath);
                Dataset<Row> products = spark.read().format("csv").option("header", true).option("inferSchema", true).load(productPath);
                Dataset<Row> orderItems = spark.read().format("csv").option("header", true).option("inferSchema", true).load(orderItemsPath);
                Dataset<Row> categories = spark.read().format("csv").option("header", true).option("inferSchema", true).load(categoryPath);
                logger.info("\n\n************************DATASET CREATED***************************\n\n");

                Dataset<Row> join1 = orders.join(orderItems, orders.col("order_id").equalTo(orderItems.col("order_item_order_id")));
                Dataset<Row> join2 = join1.join(products, join1.col("order_item_product_id").equalTo(products.col("product_id")));

                Dataset<Row> result = join2.join(categories, join2.col("product_category_id").equalTo(categories.col("category_id"))).
                        where(join2.col("order_date").like("2014-01%").
                                and(join2.col("order_status").isin("COMPLETE", "CLOSED"))).
                        groupBy(categories.col("category_id"), categories.col("category_department_id"), categories.col("category_name")).
                        agg(round(sum(join2.col("order_item_subtotal")), 2).alias("category_revenue")).
                        orderBy(col("category_id"));
                result.show();

                String path = "C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\UseCaseOutput\\UseCase4";
                result.coalesce(1).write().option("header", true).mode("overwrite").csv(path);
                logger.info("\n\n************************OUTPUT WRITTEN TO FILE***************************\n\n");
            }
            catch (Exception ex)
            {
                logger.error("ERROR=>"+ex.getMessage());
            }
        }
        else
        {
            logger.error("FILE NOT EXISTS");
        }
    }

}