import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

import java.io.File;

import static org.apache.spark.sql.functions.*;

public class UseCase2 {
        static final Logger logger=Logger.getLogger(UseCase2.class);
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
        public static void main(String[]args)
        {
            logger.info("*****************************RUNNING USECASE2**********************************");
        String orderPath = System.getenv("ORDER_PATH");
        String customerPath = System.getenv("CUSTOMER_PATH");
        if(UseCase2.checkFileExist(orderPath) && UseCase2.checkFileExist(customerPath)) {
            logger.info("******************FILES EXISTS******************");
            try {
                SparkSession spark = SparkSession.builder().master("local").getOrCreate();
                logger.info("******************SPARK SESSION CREATED**************");

                Dataset<Row> orders = spark.read().format("csv").option("header", true).option("inferSchema", true).load(orderPath);
                Dataset<Row> customers = spark.read().format("csv").option("header", true).option("inferSchema", true).load(customerPath);
                logger.info("****************************DATASET CREATED**************");

                Dataset<Row> result = customers.join(orders, orders.col("order_customer_id").equalTo(customers.col("customer_id")), "left").
                        where(orders.col("order_date").like("2014-01%").and(orders.col("order_id").isNull())).
                        select(customers.col("*")).
                        orderBy(customers.col("customer_id"));
                result.show();

                String path=System.getenv("OUTPUT_PATH")+"\\UseCase2";
                result.coalesce(1).write().option("header", true).mode("overwrite").csv(path);
                logger.info("*****************OUTPUT WRITTEN TO FILE SUCCESSFULLY*******************");
            }
            catch(Exception ex)
            {
                logger.error("ERROR=>"+ex.getMessage());
            }
        }
        else
        {
            logger.error("ERROR=>FILES NOT EXISTS");
        }

    }
}
