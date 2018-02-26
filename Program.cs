using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaProducre
{
    class Program
    {
        static void Main(string[] args)
        {

            string topic = "TripTopic";
            try
            {
                // Reads CSV file located in a path and writes to kafka topic
                using (StreamReader r = new StreamReader(@"C:\Latestbo\essls\EmplogJsons\KAFKATest\PartData.csv"))
                {
                    string json = r.ReadToEnd();
                    Message msg = new Message(json);
                    // Kafka ip
                    Uri uri = new Uri("http://14.142.119.130:9092");
                    var options = new KafkaOptions(uri);
                    var router = new BrokerRouter(options);
                    var client = new Producer(router);

                    client.SendMessageAsync(topic, new List<Message> { msg }).Wait();
                  

                }
            }
            catch(Exception e)
            { Console.Write(e.Message); }
// Wait for user key press...
            Console.ReadLine();
        }
    }
}
