/**
 * Kakfa consumer
 */

import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.time.Duration;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.Vector;
import java.util.Hashtable;
import java.util.Set;
import java.util.Map;

import java.sql.*;

public class SimpleConsumer {

   //mysql-JDBC-Driver
   static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";  
   static final String DB_URL = "jdbc:mysql://localhost:3306/school?useSSL=false&serverTimezone=UTC";
 
    //DB's name and password
   //  static final String USER = "root";
   //  static final String PWD = "Aa123456";

   // database variable
   static Connection conn = null;
   static ResultSet rs = null;
   static PreparedStatement ps=null;

   public static void main(String[] args) throws Exception {
      
      /**
       * establish a connecton 
      */

      if(args.length == 0){
         System.out.println("Enter topic name");
         return;
      }

      //Kafka consumer configuration settings
      String topicName = args[0].toString();
      Properties props = new Properties();
      
      // use to establish connection with kafka cluster(broker ip/host and port)
      props.put("bootstrap.servers", "10.86.12.127:9092,10.86.12.127:9093,10.86.12.127:9094");

      //define the name of consumer group which this consumer belong to
      props.put("group.id", "test");

      //offsets are committed automatically with a frequency controlled by the config auto.commit.interval.ms.
      props.put("enable.auto.commit", "true");
      props.put("auto.commit.interval.ms", "1000");

      //the time which consumers can disconnect from server before they are considered to have hung up
      props.put("session.timeout.ms", "30000");


      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

      KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
      
      //Kafka Consumer subscribes list of topics here.
      consumer.subscribe(Arrays.asList(topicName));
      
      //print the topic name
      System.out.println("Subscribed to topic: "+ topicName);
      

      // Hashtable<String,Vector<String[]>> hasht=new Hashtable<>();
      /**
       *     Automatic Offset Committing
       */
      while (true) {
         ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

         if( !(records.isEmpty()) ){
            conn = connectDB("root","Aa123456");

             /**
             * write to DB 
             */
            try {
               for (ConsumerRecord<String, String> record : records) {
                  // print the offset,key and value for the consumer records.
                  //System.out.printf("offset = %d, key = %s, value = %s\n",record.offset(), record.key(), record.value());
                  
                  String[] str=record.value().split("\\|");
                  // if(hasht.containsKey(str[0])){
                  //    hasht.get(str[0]).add(str);
                  // }else{
                  //     Vector<String[]> vs=new Vector<>();
                  //     vs.add(str);
                  //     hasht.put(str[0], vs);
                  // } 
                  
                  System.out.println(str[0]+" "+str[1]+" "+str[2]+" "+str[3]+" "+str[4]);
                  //check the data if it's right

                  String sql = "insert stu (Id,Name,Tel,Content,Date) values (?,?,?,?,?)";
                  ps = conn.prepareStatement(sql);
                  ps.setInt(1,Integer.parseInt(str[0]));
                  ps.setString(2,str[1]);
                  ps.setString(3,str[2]);
                  ps.setString(4,str[3]);
                  ps.setString(5,str[4]);
                  int i = ps.executeUpdate();

                  // String sql = "select Id, Name, Tel from Stu";
                  // ps = conn.prepareStatement(sql);
                  // rs = ps.executeQuery();

                  // //show  query results
                  // while(rs.next()){
                  //     //get search fields
                  //     int id  = rs.getInt("Id");
                  //     String name = rs.getString("Name");
                  //     String tel = rs.getString("Tel");
         
                  //     //output result
                  //     System.out.print("ID: " + id);
                  //     System.out.print(", name: " + name);
                  //     System.out.print(", tel: " + tel);
                  //     System.out.print("\n");
               }
            } catch (Exception e) {
               //TODO: handle exception
               e.printStackTrace();
            }finally{
               //close resource
               closeDB();
            }
         }   

         if(!(records.isEmpty())){
            System.out.println("success to receive news!!!!!");
         }   
      }
      /**
       *    Manual Offset Control
       */
    //   final int minBatchSize = 10;
    //   List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
    //   while (true) {
    //      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    //      for (ConsumerRecord<String, String> record : records) {
    //         System.out.printf("offset = %d, key = %s, value = %s\n",record.offset(), record.key(), record.value());
    //         buffer.add(record);
    //      }
    //      if (buffer.size() >= minBatchSize) {
    //          //insertIntoDb(buffer);
    //          System.out.printfln("commit!!");
    //          consumer.commitSync();
    //          buffer.clear();
    //      }
    //  }
   }

   /**
    * connect DB
    */
    public static Connection connectDB(String user,String pwd){
      Connection conn = null;
      try {

         Class.forName(JDBC_DRIVER);
        
         // open connection
         System.out.println("connect database...");
         conn = DriverManager.getConnection(DB_URL,user,pwd);
      } catch (Exception e) {
         //TODO: handle exception
         e.printStackTrace();
      }

      return conn;
    }
    
    /**
     * close connnection
     */
    public static void closeDB(){
      try{
         if(ps!=null){
             ps.close();
         } 
      }catch(SQLException se){
         //do nothing
      }
      try{
         if(rs!=null){
            rs.close(); 
         }
      }catch(SQLException se1){
         se1.printStackTrace();
      }
      try{
         if(conn!=null) {
            conn.close();
         }
      }catch(SQLException se2){
         se2.printStackTrace();
      }
    }

   //show data
   // public static void show(Hashtable<String,Vector<String[]>> show_hasht){

   //    Set<Map.Entry<String,Vector<String[]>>> entry = show_hasht.entrySet();

   //    for(Map.Entry<String,Vector<String[]>> mapEntry:entry){
   //        if(mapEntry.getKey()!=null){
   //            System.out.println("...................."+mapEntry.getKey()+"......................");
   //            System.out.println("     ID  "+"       VALUE  "+"               TIME     ");
  
   //            for(String[] arrStr:mapEntry.getValue()){
   //                for(String temp:arrStr){
   //                    System.out.printf("%-15s  ",temp);
   //                }
   //                System.out.println();
   //            }
   //            System.out.println();
   //        }else{
   //            continue;
   //        }
   //    }
   // }
}