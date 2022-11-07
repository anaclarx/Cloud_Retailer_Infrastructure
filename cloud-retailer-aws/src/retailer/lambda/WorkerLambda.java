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

		      reader.lines().forEach(line -> {
		    	try {
					String[] tempArr;
					tempArr = line.split(";");
					profit = Double.parseDouble(tempArr[6]) + profit; // get total profit by store
			    }
		    	catch(Exception ex) 
		    	  {
		    	    System.out.println(ex); 
		    	  }
		      });
			  System.out.println(profit);

		    } catch (final IOException e) {
		      System.out.println("IOException: " + e.getMessage());
		    }

		    System.out.println("Finished... processing file");
		    return "Ok";
	}
}

