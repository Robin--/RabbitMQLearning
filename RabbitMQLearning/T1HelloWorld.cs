using System;
using RabbitMQ.Client;
using System.Text;
using System.Threading.Tasks;
using System.Linq;
using System.IO;

namespace RabbitMQLearning
{
    public class T1HelloWorld
    {
        public const string QueueName = "hello";

        public static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            using (var connection = new ConnectionFactory { HostName = "localhost" }.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(QueueName, false, false, false, null);
                DemoSendAndReceiveMessageViaRabbitMQ(channel);
            }
        }

        static void DemoSendAndReceiveMessageViaRabbitMQ(IModel channel)
        {
            //
            var sendFiveTimes = 
                Enumerable
                    .Range(1, 5)
                    .Select(i => Task.Factory.StartNew( ()=>{SendHello(channel,QueueName, i);}));

            var receiveAndLog = Task.Factory.StartNew( ()=>{ConsoleLogEachMessageInQueue(channel,QueueName);});
            //
            Task.WaitAll(sendFiveTimes.ToArray());
            Task.WaitAll(receiveAndLog);
        }

        /// <param name="logger">Defaults to Console.Out</param>
        /// <param name = "listenUntil">How long to listen for. Defaults to 1 second</param>
        public static void ConsoleLogEachMessageInQueue(IModel channel, string queueName, TextWriter logger=null, TimeSpan listenUntil= default(TimeSpan))
        {
            //
            logger = logger ?? Console.Out;
            listenUntil = listenUntil == default(TimeSpan) ? TimeSpan.FromSeconds(1) : listenUntil;
            var stopWatch = new System.Diagnostics.Stopwatch();
            stopWatch.Start();
            do
            {
                var result = channel.BasicGet(queueName, true);
                if (result == null) continue;
                var message= Encoding.UTF8.GetString(result.Body);
                logger.WriteLine(message);
            }while(stopWatch.Elapsed < listenUntil);
        }

        public static void SendHello(IModel channel, string queueName, int i=0)
        {
                var message = "Test send" + i;
                var body = Encoding.UTF8.GetBytes(message);
                channel.BasicPublish("", queueName, null, body);
                Console.WriteLine(" sent  {0}", message);
        }
    }
}
