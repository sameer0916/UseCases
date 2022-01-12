import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.log4j.*;
public class Demo {
//    static final Logger logger=Logger.getLogger(Demo.class);
    public static void main(String[] args)
    {
        /*ConsoleAppender consoleAppender=new ConsoleAppender();
        consoleAppender.setThreshold(Level.ALL);
        logger.error("Log some info",new Exception());*/
        String orderPath=System.getenv("ORDER_PATH");
        SparkSession spark=SparkSession.builder().master("local").getOrCreate();
        Dataset<Row> orders=spark.read().format("csv").option("header",true).option("inferSchema",true).load(orderPath);
        orders.show();

    }
}
