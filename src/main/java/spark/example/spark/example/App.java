package spark.example.spark.example;

import static spark.Spark.get;

/**
 * Text Search
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	SparkLucene lucene = new SparkLucene();
        get("/textSearch/:searchText", (req,res)->{
            return "" + lucene.getResponse(req.params(":searchText"));
    });
    }
}
