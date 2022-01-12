import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import static org.apache.spark.sql.functions.*;
import java.io.File;
import java.io.FileFilter;
import org.apache.log4j.*;

public class UseCase1 {
    static final Logger logger=Logger.getLogger(UseCase1.class);
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
        logger.info("*****************************Running Use Case1****************************************");

        String orderPath=System.getenv("ORDER_PATH");
        String customerPath=System.getenv("CUSTOMER_PATH");
        if(UseCase1.checkFileExist(orderPath) && UseCase1.checkFileExist(customerPath))
        {
            logger.info("*************************Files Exists*******************************");
            try {
                SparkSession spark = SparkSession.builder().master("local").getOrCreate();
                logger.info("*************************Spark Session Created********************");
                Dataset<Row> orders=spark.read().format("csv").option("header",true).option("inferSchema",true).load(orderPath);
                Dataset<Row> customers=spark.read().format("csv").option("header",true).option("inferSchema",true).load(customerPath);
                logger.info("*************************Dataset Created**************************");
                Dataset<Row> result=orders.join(customers,orders.col("order_customer_id").equalTo(customers.col("customer_id"))).
                        where(orders.col("order_date").like("2014-01%")).
                        groupBy(customers.col("customer_id"),customers.col("customer_fname"),customers.col("customer_lname")).
                        agg(count(lit(1)).alias("customer_order_count")).orderBy(customers.col("customer_id"),col("customer_order_count").desc());
                result.show();
                String path="C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\UseCaseOutput\\UseCase1";
                result.coalesce(1).write().option("header",true).mode("overwrite").csv(path);
                logger.info("*************************File Saved Successfully***************");
            }
            catch(Exception ex)
            {
                logger.error("Error => "+ ex.getMessage());
            }
        }
        else
        {
            logger.error("Error => File not exist");
        }
    }
}
