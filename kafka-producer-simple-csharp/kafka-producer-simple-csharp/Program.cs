using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace kafka_producer_simple_csharp
{
    public class Program
    {
        public static async Task Main()
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            // If serializers are not specified, default serializers from
            // `Confluent.Kafka.Serializers` will be automatically used where
            // available. Note: by default strings are encoded as UTF8.
            using var p = new ProducerBuilder<Null, string>(config).Build() ;

            {
                try
                {
                    var count = 0;
                    while(true)
                    {
                        var dr = await p.ProduceAsync("test-topic", 
                            new Message<Null, string> { Value = $"test: { count }" });

                        Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset} | { count }'");

                        count++;
                        Thread.Sleep(2000);
                    }
                    
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }

    }
}
