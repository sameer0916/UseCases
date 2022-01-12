import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

import java.io.File;

import static org.apache.spark.sql.functions.*;

public class UseCase5 {
    static final Logger logger=Logger.getLogger(UseCase5.class);
    public static boolean checkFileExist(String path)
    { try {
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
        logger.info("****************************************RUNNING USECASE5**********************************\n\n\n");

        String departmentPath = System.getenv("DEPARTMENT_PATH");
        String productPath = System.getenv("PRODUCT_PATH");
        String categoryPath = System.getenv("CATEGORY_PATH");
        if(UseCase3.checkFileExist(departmentPath) && UseCase3.checkFileExist(productPath)&& UseCase3.checkFileExist(categoryPath)) {
            logger.info("\n\n************************FILE EXISTS***************************\n\n");
            try {
                SparkSession spark = SparkSession.builder().master("local").getOrCreate();
                logger.info("\n\n************************SPARK SESSION CREATED***************************\n\n");

                Dataset<Row> departments = spark.read().format("csv").option("header", true).option("inferSchema", true).load(departmentPath);
                Dataset<Row> products = spark.read().format("csv").option("header", true).option("inferSchema", true).load(productPath);
                Dataset<Row> categories = spark.read().format("csv").option("header", true).option("inferSchema", true).load(categoryPath);
                logger.info("\n\n************************DATASET CREATED***************************\n\n");

                Dataset<Row> result = products.join(categories, products.col("product_category_id").equalTo(categories.col("category_id"))).
                        join(departments, categories.col("category_department_id").equalTo(departments.col("department_id"))).
                        groupBy(departments.col("department_id"), departments.col("department_name")).
                        agg(count(products.col("product_id")).alias("product_count")).
                        orderBy(col("department_id"));
                result.show();

                String path=System.getenv("OUTPUT_PATH")+"\\UseCase5";
                result.coalesce(1).write().option("header", true).mode("overwrite").csv(path);
                logger.info("\n\n************************OUTPUT WRITTEN TO FILE***************************\n\n");
            }
            catch(Exception ex)
            {
                logger.error("ERROR=>"+ ex.getMessage());
            }
        }
        else
        {
            logger.error("~~~~~~~~~~~~~~~~~~~~~~~FILE NOT PRESENT~~~~~~~~~~~~~~~~~~~");
        }
    }
}
