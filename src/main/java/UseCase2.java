import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

import java.io.File;

import static org.apache.spark.sql.functions.*;

public class UseCase2 {
        public static boolean checkFileExist(String path)
        {
            File file=new File(path);
            return file.exists();
        }
        public static void main(String[]args)
        {
        String orderPath = "C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";
        String customerPath = "C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\customers\\part-00000";
        if(UseCase2.checkFileExist(orderPath) && UseCase2.checkFileExist(customerPath)) {
            SparkSession spark = SparkSession.builder().master("local").getOrCreate();
            Dataset<Row> orders = spark.read().format("csv").option("header", true).option("inferSchema", true).load(orderPath);
            Dataset<Row> customers = spark.read().format("csv").option("header", true).option("inferSchema", true).load(customerPath);
            Dataset<Row> result = customers.join(orders, orders.col("order_customer_id").equalTo(customers.col("customer_id")), "left").
                    where(orders.col("order_date").like("2014-01%").and(orders.col("order_id").isNull())).
                    select(customers.col("*")).
                    orderBy(customers.col("customer_id"));
            result.show();
            String path = "C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\UseCaseOutput\\UseCase2";
            result.coalesce(1).write().option("header", true).option("overwrite", true).csv(path);
        }
        else
        {
            System.out.println("Path Not Exists");
        }

    }
}
