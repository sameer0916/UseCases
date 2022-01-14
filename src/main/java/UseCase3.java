import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

import java.io.File;

import static org.apache.spark.sql.functions.*;

public class UseCase3 {
    static final Logger logger=Logger.getLogger(UseCase3.class);
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
    public static boolean validateResult(long count)
    {
        if(count==1941)
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    public static void main(String[] args)
    {
        logger.info("****************************************RUNNING USECASE3**********************************\n\n\n");
        String orderPath = System.getenv("ORDER_PATH");
        String customerPath = System.getenv("CUSTOMER_PATH");
        String orderItemsPath=System.getenv("ORDER_ITEM_PATH");
        if(UseCase3.checkFileExist(orderPath) && UseCase3.checkFileExist(customerPath)&& UseCase3.checkFileExist(orderItemsPath)) {
            logger.info("\n\n************************FILE EXISTS***************************\n\n");
            try {
                SparkSession spark = SparkSession.builder().master("local").getOrCreate();
                logger.info("\n\n************************SPARK SESSION CREATED***************************\n\n");

                Dataset<Row> orders = spark.read().format("csv").option("header", true).option("inferSchema", true).load(orderPath);
                Dataset<Row> customers = spark.read().format("csv").option("header", true).option("inferSchema", true).load(customerPath);
                Dataset<Row> orderItems = spark.read().format("csv").option("header", true).option("inferSchema", true).load(orderItemsPath);
                logger.info("\n\n************************DATASET CREATED***************************\n\n");

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
                long count=result.count();
                if(validateResult(count))
                {
                    logger.info("***********************RESULT VALIDATED*******************");
                    String path=System.getenv("OUTPUT_PATH")+"\\UseCase3";
                    result.coalesce(1).write().option("header", true).mode("overwrite").csv(path);
                    logger.info("\n\n************************OUTPUT WRITTEN TO FILE***************************\n\n");
                }
                else
                {
                    logger.error("COUNT NOT MATCHED");
                }

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
