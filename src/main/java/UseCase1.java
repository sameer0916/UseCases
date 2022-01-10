import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import static org.apache.spark.sql.functions.*;

public class UseCase1 {
    public static void main(String[]args)
    {
        String orderPath="C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";
        String customerPath="C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\customers\\part-00000";
        SparkSession spark=SparkSession.builder().master("local").getOrCreate();
        Dataset<Row> orders=spark.read().format("csv").option("header",true).option("inferSchema",true).load(orderPath);
        Dataset<Row> customers=spark.read().format("csv").option("header",true).option("inferSchema",true).load(customerPath);
        Dataset<Row> result=orders.join(customers,orders.col("order_customer_id").equalTo(customers.col("customer_id"))).
                where(orders.col("order_date").like("2014-01%")).
                groupBy(customers.col("customer_id"),customers.col("customer_fname"),customers.col("customer_lname")).
                agg(count(lit(1)).alias("customer_order_count")).orderBy(customers.col("customer_id"),col("customer_order_count").desc());
        result.show();
        String path="C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\UseCaseOutput\\UseCase1";
        result.coalesce(1).write().option("header",true).option("overwrite",true).csv(path);
    }
}
