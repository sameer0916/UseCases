import org.junit.Test;

import static org.junit.Assert.assertFalse;

import static org.junit.Assert.assertTrue;

public class UseCase2Test {
    @Test
    public void TestPathExists()
    {
        String orderPath="C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";

        assertTrue(UseCase2.checkFileExist(orderPath));
    }
    @Test
    public void TestPathNotExists()
    {
        String orderPath="C:\\Users\\Sameer Mittal\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-000001";
        assertFalse(UseCase2.checkFileExist(orderPath));
    }
    @Test
    public void ValidResult()
    {
        long count=0;
        assertTrue(UseCase2.validateResult(count));
    }
    @Test
    public void NotValidResult()
    {
        long count = 4;
        assertFalse(UseCase2.validateResult(count));
    }
}
