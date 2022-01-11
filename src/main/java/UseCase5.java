import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

import java.io.File;

import static org.apache.spark.sql.functions.*;

public class UseCase5 {
    public static boolean checkFileExist(String path)
    {
        File file=new File(path);
        return file.exists();
    }
    public static void main(String[]args)
    {
        String departmentPath = "C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\departments\\part-00000";
        String productPath = "C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\products\\part-00000";
        String categoryPath = "C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\categories\\part-00000";
        if(UseCase3.checkFileExist(departmentPath) && UseCase3.checkFileExist(productPath)&& UseCase3.checkFileExist(categoryPath)) {
            SparkSession spark = SparkSession.builder().master("local").getOrCreate();

            Dataset<Row> departments = spark.read().format("csv").option("header", true).option("inferSchema", true).load(departmentPath);
            Dataset<Row> products = spark.read().format("csv").option("header", true).option("inferSchema", true).load(productPath);
            Dataset<Row> categories = spark.read().format("csv").option("header", true).option("inferSchema", true).load(categoryPath);

            Dataset<Row> result = products.join(categories, products.col("product_category_id").equalTo(categories.col("category_id"))).
                    join(departments, categories.col("category_department_id").equalTo(departments.col("department_id"))).
                    groupBy(departments.col("department_id"), departments.col("department_name")).
                    agg(count(products.col("product_id")).alias("product_count")).
                    orderBy(col("department_id"));
            result.show();

            String path = "C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\UseCaseOutput\\UseCase5";
            result.coalesce(1).write().option("header", true).option("overwrite", true).csv(path);
        }
        else
        {
            System.out.println("Path Not Exists");
        }
    }
}
