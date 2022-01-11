import org.junit.Test;

import static org.junit.Assert.assertFalse;

import static org.junit.Assert.assertTrue;

public class UseCase3Test {
    @Test
    public void TestPathExists()
    {
        String orderPath="C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";

        assertTrue(UseCase3.checkFileExist(orderPath));
    }
    @Test
    public void TestPathNotExists()
    {
        String orderPath="C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-000001";
        assertFalse(UseCase3.checkFileExist(orderPath));
    }
}
