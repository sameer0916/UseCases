import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

import java.io.File;

import static org.apache.spark.sql.functions.*;

public class UseCase3 {
    public static boolean checkFileExist(String path)
    {
        File file=new File(path);
        return file.exists();
    }
    public static void main(String[] args)
    {
        String orderPath = "C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";
        String customerPath = "C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\customers\\part-00000";
        String orderItemsPath="C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\order_items\\part-00000";
        if(UseCase3.checkFileExist(orderPath) && UseCase3.checkFileExist(customerPath)&& UseCase3.checkFileExist(orderItemsPath)) {
            SparkSession spark = SparkSession.builder().master("local").getOrCreate();

            Dataset<Row> orders = spark.read().format("csv").option("header", true).option("inferSchema", true).load(orderPath);
            Dataset<Row> customers = spark.read().format("csv").option("header", true).option("inferSchema", true).load(customerPath);
            Dataset<Row> orderItems = spark.read().format("csv").option("header", true).option("inferSchema", true).load(orderItemsPath);

            Dataset<Row> joinedDataset = customers.join(orders, orders.col("order_customer_id").equalTo(customers.col("customer_id")));

            Dataset<Row> result = joinedDataset.join(orderItems, joinedDataset.col("order_id").equalTo(orderItems.col("order_item_order_id"))).
                    where(joinedDataset.col("order_date").like("2014-01%").
                            and(joinedDataset.col("order_status").isin("COMPLETE", "CLOSED"))).
                    groupBy(joinedDataset.col("customer_id"), joinedDataset.col("customer_fname"),
                            joinedDataset.col("customer_lname")).
                    agg(round(sum(orderItems.col("order_item_subtotal")), 2).alias("customer_revenue")).
                    select(col("customer_id"), col("customer_fname").alias("customer_first_name"), col("customer_lname").
                            alias("customer_last_name"), coalesce(col("customer_revenue")).alias("customer_revenue")).
                    orderBy(col("customer_id"), col("customer_revenue").desc());

            result.show();
            String path = "C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\UseCaseOutput\\UseCase3";
            result.coalesce(1).write().option("header", true).option("overwrite", true).csv(path);
        }
        else
        {
            System.out.println("Path Not exists");
        }

    }
}
