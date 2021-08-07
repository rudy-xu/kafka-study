using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace producer
{
    class Program
    {
        //would like to wait for the result of your produce requests before proceeding.
        // static async Task Main(string[] args)
        // {
        //     var conf = new ProducerConfig 
        //     { 
        //         BootstrapServers = "10.86.12.127:9092,10.86.12.127:9093,10.86.12.127:9094" 
        //     };

        //     using (var p = new ProducerBuilder<Null, string>(conf).Build())
        //     {
        //         try
        //         {
        //             for (var i = 1; i <= 10; i++)
        //             {
        //                 var result = await p.ProduceAsync("mykafka0520", new Message<Null, string> { Value = $"my message: {i}" });
        //                 Console.WriteLine($"Delivered '{result.Value}'");
        //             }

        //         }
        //         catch (ProduceException<Null, string> e)
        //         {
        //             Console.WriteLine($"Delivery failed: {e.Error.Reason}");
        //         }
        //     }
        //     Console.WriteLine("Done!");
        //     Console.ReadKey();   //等待用户按下任意键，一次读入一个字符；类似于C++中的System("pause");使得控制台窗口停留一下。直到敲击键盘为止）
        // }

        public static void Main(string[] args)
        {
            var conf = new ProducerConfig 
            { 
                BootstrapServers = "10.86.5.205:9092,10.86.5.205:9093,10.86.5.205:9094" 
            };

            Action<DeliveryReport<Null, string>> handler = r => 
                Console.WriteLine(!r.Error.IsError ? $"Delivered message to {r.TopicPartitionOffset}" : $"Delivery Error: {r.Error.Reason}");

            using (var producer = new ProducerBuilder<Null, string>(conf).Build())
            {
                
                

                for (int i=3; i<=103; ++i)
                {
                    string Id = i.ToString();
                    string Name = "kindle" + Id;
                    string Tel = "01010"+Id+Id+Id;
                    string Work = Name+"tom435";
                    string news = Id+"|"+Name+"|"+Tel+"|"+Work+"|"+"2020-05-28";
                    producer.Produce("mykafka0528", new Message<Null, string> { Value = news }, handler);

                    Thread.Sleep(2000);

                }

                // wait for up to 10 seconds for any inflight messages to be delivered.
                producer.Flush(TimeSpan.FromSeconds(10));
            }
        }
    }
}
