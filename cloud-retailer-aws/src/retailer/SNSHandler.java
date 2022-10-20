package retailer;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.SnsException;

public class SNSHandler {

  public static void main(String[] args) {
    Region region = Region.US_EAST_1;

    if (args.length < 3) {
      System.out.println("Missing the Topic ARN, Bucket Name, or File Name arguments");
      System.exit(1);
    }

    String topicARN = args[0];
    String bucketName = args[1];
    String fileName = args[2];

    try {
      SnsClient snsClient = SnsClient.builder().region(region).build();

      PublishRequest request = PublishRequest.builder().message(bucketName + ";" + fileName).topicArn(topicARN)
          .build();

      PublishResponse snsResponse = snsClient.publish(request);
      System.out.println(
          snsResponse.messageId() + " Message sent. Status is " + snsResponse.sdkHttpResponse().statusCode());

    } catch (SnsException e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }
  }
}