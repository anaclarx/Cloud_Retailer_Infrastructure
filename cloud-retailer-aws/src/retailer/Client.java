package retailer;

import java.io.File;
 
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import retailer.SNSHandler;
 
public class Client {
    public static void main(String[] args) {
    	
    	File dir = new File("/Users/caca/Desktop/França/Cloud/Cloud_Retailer_Infrastructure/sales-data/");
    	
    	File[] directoryListing = dir.listFiles();
    	
    	if (directoryListing != null) {
    		for (File child : directoryListing) {
    			
    			SNSHandler sns = new SNSHandler();
    			
    			String bucketName = "retailerbucket2212";
    	        
    	        String fileName = child.getName();
    	        
    	        String[] snsArgs = new String[]{"arn:aws:sns:us-east-1:148253347322:retailer-data-topic", bucketName, fileName};
    	         
    	        String filePath = "/Users/caca/Desktop/França/Cloud/Cloud_Retailer_Infrastructure/sales-data/" + fileName;
    	         
    	        S3Client client = S3Client.builder().build();
    	         
    	        PutObjectRequest request = PutObjectRequest.builder()
    	                            .bucket(bucketName).key("sales-data/" + fileName).build();
    	         
    	        client.putObject(request, RequestBody.fromFile(new File(filePath)));
    	      
    	        
    	        sns.main(snsArgs);
    	        
    		 }
    	} else {
    		return;
    		  }

                 
    }
}



  
  