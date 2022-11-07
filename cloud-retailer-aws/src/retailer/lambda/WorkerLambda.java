package retailer.lambda;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
public class WorkerLambda implements RequestHandler<S3Event, String> {
	
	  public WorkerLambda() {}

	  Double profit = 0.0;

	  public String handleRequest(S3Event event, Context context) {
		    S3EventNotificationRecord record = event.getRecords().get(0);
		    String bucketName = record.getS3().getBucket().getName();
		    String fileKey = record.getS3().getObject().getUrlDecodedKey();

		    AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
		    try (final S3Object s3Object = s3Client.getObject(bucketName, fileKey);
		        final InputStreamReader streamReader = new InputStreamReader(s3Object.getObjectContent(),
		            StandardCharsets.UTF_8);
		        final BufferedReader reader = new BufferedReader(streamReader)) {

		      Integer[] values = new Integer[4];
		      values[0] = 0; // total profit for each store
		      values[1] = 0; // count
		      values[2] = Integer.MAX_VALUE; // min
		      values[3] = Integer.MIN_VALUE; // max

		      reader.lines().forEach(line -> {
		    	try {
					String[] tempArr;
					tempArr = line.split(";");
					profit = Double.parseDouble(tempArr[6]) + profit;
			    }
		    	catch(Exception ex) 
		    	  {
		    	     System.out.println(ex); 
		    	  }
		      });
			  System.out.println(profit);
		      

		      System.out.println("Count: " + values[1] + " Sum: " + values[0] + " Avg: " + values[0] / (double) values[1]
		          + " Min: " + values[2] + " Max: " + values[3]);
		    } catch (final IOException e) {
		      System.out.println("IOException: " + e.getMessage());
		    }

		    System.out.println("Finished... processing file");
		    return "Ok";
	}
}

