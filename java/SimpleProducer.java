/**
 * kafka producer
 */
//import util.properties packages
import java.util.Properties;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

//Create java class named “SimpleProducer"
public class SimpleProducer {
   
   public static void main(final String[] args) throws Exception {

      // Check arguments length value
      if (args.length == 0) {
         System.out.println("Enter topic name");
         return;
      }
      //File msgFile=new File("C:\\sioux_rl\\Sioux\\work\\docker\\testFile\\message.txt");

      // Assign topicName to string variable
      String topicName = args[0].toString();

      //String topicName = "mykafka";

      // create instance for properties to access producer configs
      Properties props = new Properties();

      // use to establish connection with kafka cluster(broker ip/host and port)
      props.put("bootstrap.servers", "10.86.12.127:9092,10.86.12.127:9093,10.86.12.127:9094");

      // (leader response mechanism)Set acknowledgements for producer requests.
      props.put("acks", "all");

      // If the request fails, the producer can automatically retry,
      props.put("retries", 0);

      // Reduce the no of requests less than 0
      props.put("linger.ms", 1); // (1ms delay)

      // Specify buffer size in config (When mutiple message are sent to the same
      // partition, the producer attempts to pack the message together)
      props.put("batch.size", 16384);

      // The buffer.memory controls the total amount of memory available to the
      // producer for buffering.
      props.put("buffer.memory", 33554432);

      // StringSerializer is turn key or value into string or bytes
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

      System.out.println("....................prepare to and send message..................");
      Producer<String, String> producer = new KafkaProducer<String, String>(props);
	
      for(int i=5;i<50;++i){
      	try{
               Thread.sleep(1000);
            }catch(Exception e){
               e.printStackTrace();
            }
            
            String Id=Integer.toString(i);
            String Name="kenda"+Id;
            String Tel="000"+Id+Id+Id+Id;
            String Content=Name+"sdsd";
            String news=Id+"|"+Name+"|"+Tel+"|"+Content+"|"+"2020-05-20";

        producer.send(new ProducerRecord<String, String>(topicName,news));
      }      

    //   send(msgFile,topicName,props);

       producer.close();

      System.out.println("Message sent successfully");
      // producer.close();
    }

    // public static void send(File file,String send_topic,Properties send_props){

    //   FileInputStream fileIns=null;
    //   InputStreamReader insr=null;
    //   BufferedReader br=null;

    //   if(file.exists()){
    //       try{
    //           String buffer=null;
    //           fileIns=new FileInputStream(file);
    //           insr=new InputStreamReader(fileIns);
    //           br=new BufferedReader(insr);
              
    //           Producer<String, String> producer = new KafkaProducer<String, String>(send_props);
    //           while((buffer=br.readLine())!=null){
    //                  producer.send(new ProducerRecord<String, String>(send_topic,buffer));
    //            }
    //            producer.close();
    //       }catch(Exception e){
    //           e.printStackTrace();
    //       }finally{
    //           try{
    //              br.close();
    //           }catch(Exception e1){
    //              e1.printStackTrace();
    //           }
    //       }
    //    }else{
    //	      System.out.println("File is not exits, please check it.");
    //    }
    //  }
   }
