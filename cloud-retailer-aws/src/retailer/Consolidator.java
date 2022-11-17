package retailer;

import java.nio.charset.StandardCharsets;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Consolidator {
    public static void main(String[] args) {
        Region region = Region.US_EAST_1;

        
        System.out.println("ENTRANDO NA FUNCAO");
        
        
        String bucketName = "summaryretailerbucket2212";
        
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        
        S3Client s3 = S3Client.builder().region(region).build();
        
        if (args.length < 1) {
            System.out.println("Missing the file date argument");
            System.exit(1);
        }
        
    	ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(bucketName)
                .withPrefix(args[0]);

    	List<String> keys = new ArrayList<>();

    	ObjectListing objects = s3Client.listObjects(listObjectsRequest);

    	for (;;) {
    		List<S3ObjectSummary> summaries = objects.getObjectSummaries();
    		if (summaries.size() < 1) {
    			break;
    		}

    		summaries.forEach(s -> keys.add(s.getKey()));
    		objects = s3Client.listNextBatchOfObjects(objects);
    	}
    	
    	
    	for (String element : keys) {

                // Retrieve file
                GetObjectRequest objectRequest = GetObjectRequest.builder().key(element)
                    .bucket(bucketName).build();

                ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
                byte[] data = objectBytes.asByteArray();

                File file = new File(element);
                try (OutputStream os = new FileOutputStream(file)) {
                    os.write(data);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                // read what we wrote
                String dataString = new String(data, StandardCharsets.UTF_8);
                System.out.println(dataString);

                // manipulating the data
                String[] tempArray = dataString.split("\\n");
                String[][] dataArray = new String[tempArray.length][6];
                for (int i =0; i < tempArray.length; i++){
                    dataArray[i] = tempArray[i].split(";");
                }
                
                System.out.println(dataArray);
                
            }
    	}

    }
