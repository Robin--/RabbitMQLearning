using NUnit.Framework;
using System;
using System.Diagnostics;
using RabbitMQ.Client;
using System.Text;
using System.IO;

namespace RabbitMQLearning.Tests
{
    [TestFixture]
    public class T1HelloWorldTests
    {
        [Test]
        public void Ensure_Local_RabbitMQ()
        {
            try
            {
                using (var conn = new ConnectionFactory{HostName = "localhost"}.CreateConnection())
                {}
            }
            catch(IOException e)
            {
                Assert.Fail(
                    "Couldn't create a connection to RabbitMQ on localhost. Is rabbitmq-server running? \nException:\b {0}",
                    e
                );
            }
        }


        [Test]
        public void Send_should_send_a_non_empty_string()
        {
            //A
            string result="";

            RunWithRabbitLocalChannelAndQueue(
                "Send_should_send", 
                (channel,queueName) => 
                {
                    //A
                    T1HelloWorld.SendHello(channel, queueName);
                    result= ListenForFirstMessage(channel, queueName);
                }
            );

            //A
            Assert.Greater(result.Length, 0);
        }

        [Test]
        public void Receive_should_receive_whatever_was_sent()
        {
            //Expect
            string testSend = "Test Send " + Guid.NewGuid();

            using (var writer = new StringWriter())
            {
                //A
                RunWithRabbitLocalChannelAndQueue(
                    "Receive_should_receive",
                    (c,q)=>{
                        c.BasicPublish("",q,null,Encoding.UTF8.GetBytes(testSend));
                        T1HelloWorld.ConsoleLogEachMessageInQueue(c, q, writer, TimeSpan.FromMilliseconds(250));
                    }
                );

                //A
                Assert.That(writer.ToString(), Contains.Substring(testSend));
            }
        }

        void RunWithRabbitLocalChannelAndQueue(string queueName, Action<IModel,string> action)
        {
            using (var conn = new ConnectionFactory{HostName = "localhost"}.CreateConnection())
            using (var channel = CreateLocalChannel(conn, queueName))
            {
                action(channel,queueName);
            }
        }

        IModel CreateLocalChannel(IConnection connection, string name)
        {
            var channel = connection.CreateModel();
            channel.QueueDeclare(name,false,false,false,null);
            return channel;
        }

        string ListenForFirstMessage(IModel channel, string queueName, long timeoutMillis=5000)
        {
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            do
            {
                var result= channel.BasicGet(queueName, false);
                if (result==null) continue;
                var message = Encoding.UTF8.GetString(result.Body);
                return message;
            }while(stopWatch.Elapsed.TotalMilliseconds < timeoutMillis);

            throw new TimeoutException(
                string.Format("Didn't get any messages at all on queue {0} within timeout {1}",queueName,timeoutMillis)
                );
        }
    }
}

