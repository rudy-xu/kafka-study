using System;
using System.Threading;
using Confluent.Kafka;
using MySql.Data.MySqlClient;

namespace consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var conf = new ConsumerConfig
            { 
                GroupId = "test",
                BootstrapServers = "10.86.5.205:9092,10.86.5.205:9093,10.86.5.205:9094",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // earliest message in the topic 'my-topic' the first time you run the program.
                AutoCommitIntervalMs = 1000,
                EnableAutoCommit = true
            };

            //connect db
            string con_url="server=localhost;User=root;password=Aa123456;database=company";

            //ConsumerBuilder<Tkey,TValue> 初始化一个ConsumerBuilder实例(用于构建消费者)
            //Ignore是一个sealed类(有点类似与枚举)，使得消息key或者value可以为空值
            using (var consumer = (new ConsumerBuilder<Ignore, string>(conf)).Build())
            {
                consumer.Subscribe("mykafka0528");

                CancellationTokenSource cts = new CancellationTokenSource();    //向应该被取消的CancellationToken发送信号

                //截获按下Ctrl+C组合键信号，以便时间除了程序可以决定是继续执行还是终止
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();    //cancel request
                };

                try
                {
                    Console.WriteLine("-----------------------Prepared to receive message---------------------");
                    while (true)
                    {
                        try
                        {
                            //pull for new messages/events  (Token--获取CancellationToken)
                            var records = consumer.Consume(cts.Token);
                            Console.WriteLine($"Consumed message '{records.Message.Value}' at: '{records.TopicPartitionOffset}'.");                           

                            if(!(records.Message.Value == "")){
                                string[] str = records.Message.Value.Split('|');
                                //MySqlDataReader dr=null;
                                MySqlConnection conn=null;
                                try{
                                    conn = new MySqlConnection(con_url);

                                    string sql="insert staff (id,name,tel,work,Date) values (@id,@name,@tel,@content,@date)";
                                    MySqlCommand cmd = new MySqlCommand(sql,conn);
                                    cmd.Parameters.Add(new MySqlParameter("@id",str[0]));
                                    cmd.Parameters.Add(new MySqlParameter("@name",str[1]));
                                    cmd.Parameters.Add(new MySqlParameter("@tel",str[2]));
                                    cmd.Parameters.Add(new MySqlParameter("@content",str[3]));
                                    cmd.Parameters.Add(new MySqlParameter("@date",str[4]));

                                    conn.Open();
                                    cmd.ExecuteNonQuery();
                                    // dr = cmd.ExecuteReader();

                                    // Console.WriteLine("----------------------------------------------------------------------------------");
                                    // while(dr.Read() == true){
                                    //     string id  = dr["Id"].ToString();
                                    //     string name = dr["Name"].ToString();
                                    //     string tel = dr["Tel"].ToString();
                                    //     string content = dr["Content"].ToString();
                                    //     DateTime dt = (DateTime)dr["Date"];
                                    //     string date = dt.ToString("yyyy-MM-dd");
                                    //     //Console.WriteLine($"ID: {id} name: {name} tel: {tel} content: {content} date: {date}");
                                    //     Console.WriteLine($"ID: {string.Format("{0,-3}",id)} name: {string.Format("{0,-9}",name)} tel: {string.Format("{0,-12}",tel)} content: {string.Format("{0,-13}",content)} date: {date}");
                                    // }
                                    // Console.WriteLine("----------------------------------------------------------------------------------");
  
                                }catch(MySqlException ex){
                                    Console.WriteLine(ex.Message);
                                }finally{
                                    // dr.Close();
                                    conn.Close();
                                }

                            }
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }catch (OperationCanceledException){
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    consumer.Close();
                }
            }
        }
    }
}
