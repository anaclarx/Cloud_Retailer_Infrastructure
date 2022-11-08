package retailer;

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
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

public class Consolidator {
    public static void main(String[] args) {
        Region region = Region.US_EAST_1;

        String bucketName = "retailerbucket2212";
        String fileDate = args[0];

        S3Client s3 = S3Client.builder().region(region).build();

        // Check file exists
        ListObjectsRequest listObjects = ListObjectsRequest.builder()
            .bucket(bucketName).build();

        ListObjectsResponse res = s3.listObjects(listObjects);
        List<S3Object> objects = res.contents();

        if (objects.stream().anyMatch((S3Object x) -> x.key().equals(fileDate))) {

        // Retrieve file
        GetObjectRequest objectRequest = GetObjectRequest.builder().key(fileDate)
            .bucket(bucketName).build();

        ResponseBytes<
            GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
        byte[] data = objectBytes.asByteArray();

        File file = new File(fileDate);
        try (OutputStream os = new FileOutputStream(file)) {
            os.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }

        }
    }
}
