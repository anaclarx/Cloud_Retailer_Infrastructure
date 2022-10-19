package retailer;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;

public class S3ListBuckets {

  public static void main(String[] args) {
    Region region = Region.US_EAST_1;

    S3Client s3 = S3Client.builder().region(region).build();

    ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder()
        .build();
    ListBucketsResponse listBucketResponse = s3.listBuckets(listBucketsRequest);
    listBucketResponse.buckets().stream()
        .forEach(x -> System.out.println(x.name()));
  }
}