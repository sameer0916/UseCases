import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

import java.io.File;

import static org.apache.spark.sql.functions.*;

public class UseCase4 {
    public static boolean checkFileExist(String path)
    {
        File file=new File(path);
        return file.exists();
    }
    public static void main(String[] args) {

        String orderPath = "C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";
        String productPath = "C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\products\\part-00000";
        String orderItemsPath = "C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\order_items\\part-00000";
        String categoryPath = "C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\categories\\part-00000";
        if(UseCase4.checkFileExist(orderPath) && UseCase4.checkFileExist(productPath)&& UseCase4.checkFileExist(orderItemsPath)&& UseCase4.checkFileExist(categoryPath)) {
            SparkSession spark = SparkSession.builder().master("local").getOrCreate();

            Dataset<Row> orders = spark.read().format("csv").option("header", true).option("inferSchema", true).load(orderPath);
            Dataset<Row> products = spark.read().format("csv").option("header", true).option("inferSchema", true).load(productPath);
            Dataset<Row> orderItems = spark.read().format("csv").option("header", true).option("inferSchema", true).load(orderItemsPath);
            Dataset<Row> categories = spark.read().format("csv").option("header", true).option("inferSchema", true).load(categoryPath);

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
            result.coalesce(1).write().option("header", true).option("overwrite", true).csv(path);
        }
        else
        {
            System.out.println("Path not exists");
        }
    }

}