package hello;

import com.amazonaws.services.sqs.model.*;
import org.joda.time.LocalTime;
import com.amazonaws.services.sqs.*;
import com.amazonaws.auth.*;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import java.util.*;
import java.util.Map.Entry;


public class HelloWorld {
    public static void main(String[] args) {

        String awsKey = "";         // Must be set
        String awsSecret = "";      //  Must be set
        String queueArn = "";       // Must be set


        // Bacis hello world stuff
        LocalTime currentTime = new LocalTime();
        System.out.println("The current local time is: " + currentTime);
        Greeter greeter = new Greeter();
        System.out.println(greeter.sayHello());


        //  Make an aws connection to an SQS account
        AmazonSQS sqs = new AmazonSQSClient(new BasicAWSCredentials(awsKey, awsSecret));
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);


        //  Get a list of the queues and output them to the screen
        ListQueuesResult queueList;
        queueList = sqs.listQueues();

        System.out.println("Searching for queues:");
        for (String queueUrl: queueList.getQueueUrls() ) {
            System.out.println(" + Found queue at url " + queueUrl);
        }
        System.out.println();



        //  Get a handle to the queue we need
        System.out.println("Creating a new SQS queue called MyQueue.\n");
        CreateQueueRequest createQueueRequest = new CreateQueueRequest("dsmurl-file-ingest-queue");
        String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();


        // Send a message
        System.out.println("Sending a message to MyQueue.\n");
        sqs.sendMessage(new SendMessageRequest(myQueueUrl, "This is my message text."));


        // Receive messages
        while (true) {

            System.out.println("*****************************************\n");
            System.out.println("**  Receiving messages from MyQueue.   **\n");
            System.out.println();

            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            System.out.println();
            System.out.println("...");

            for (Message message : messages) {
                System.out.println("  Message");
                System.out.println("    MessageId:     " + message.getMessageId());
                System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
                System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
                System.out.println("    Body:          " + message.getBody());
                for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                    System.out.println("  Attribute");
                    System.out.println("    Name:  " + entry.getKey());
                    System.out.println("    Value: " + entry.getValue());
                }

                sqs.deleteMessage(myQueueUrl, message.getReceiptHandle());   // delete the message so it doesn't get processed twice
            }

            System.out.println("*****************************************\n");
            System.out.println();

            try {
                Thread.sleep(5000);
            } catch (Exception e) {
                System.out.println("Error log: " + e.toString());
            }
        }
    }
}

