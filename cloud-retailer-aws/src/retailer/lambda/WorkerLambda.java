package retailer.lambda;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import java.util.Hashtable;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
public class WorkerLambda implements RequestHandler<S3Event, String> {
	
	  public WorkerLambda() {}

	  Hashtable<String, Double> profit = new Hashtable<String, Double>();

	  Hashtable<String, Integer> quantity = new Hashtable<String, Integer>();

	  Hashtable<String, Double> sold = new Hashtable<String, Double>();

	  Hashtable<String, Double> productsProfit = new Hashtable<String, Double>();

	  public String handleRequest(S3Event event, Context context) {
		    S3EventNotificationRecord record = event.getRecords().get(0);
		    String bucketName = record.getS3().getBucket().getName();
		    String fileKey = record.getS3().getObject().getUrlDecodedKey();

		    AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
		    try (final S3Object s3Object = s3Client.getObject(bucketName, fileKey);
		        final InputStreamReader streamReader = new InputStreamReader(s3Object.getObjectContent(),
		            StandardCharsets.UTF_8);
		        final BufferedReader reader = new BufferedReader(streamReader)) {

		      reader.lines().forEach(line -> {
		    	try {
					String[] tempArr;
					tempArr = line.split(";");

					// get total profit by store
					profit.put(tempArr[1], Double.parseDouble(tempArr[6])*Integer.parseInt(tempArr[3]) + profit.get(tempArr[1]));

					// get total quantity by product by store
					quantity.put(tempArr[2], quantity.get(tempArr[2]) + Integer.parseInt(tempArr[3]));

					// get total sold by product by store
					sold.put(tempArr[2], sold.get(tempArr[2]) + Integer.parseInt(tempArr[7]));

					// get total profit by product by store
					productsProfit.put(tempArr[2], productsProfit.get(tempArr[2]) + Integer.parseInt(tempArr[6]));

			    }
		    	catch(Exception ex) 
		    	  {
		    	    System.out.println(ex); 
		    	  }
		      });
			  System.out.println(profit);
			  System.out.println(quantity);

		    } catch (final IOException e) {
		      System.out.println("IOException: " + e.getMessage());
		    }

		    System.out.println("Finished... processing file");
		    return "Ok";
	}
}

