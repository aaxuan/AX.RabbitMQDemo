using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace AX.TestConsole
{
    public class MQProgram_Simple
    {
        public void Run()
        {
            Console.WriteLine(GetType().FullName);
            Produce();
            Consumer();
        }

        public void Produce()
        {
            var conn = GetMQConn($"{GetType().Name}_生产者");
            var channel = conn.CreateModel();
            string queueName = $"{GetType().Name}_生产者";

            try
            {
                //参数：队列名称，是否持久化，是否独占独立，是否自动删除，附属参数
                channel.QueueDeclare(queueName, false, false, false, null);
                var count = new System.Random().Next(1, 50);
                Console.WriteLine(count);
                for (int i = 0; i < count; i++)
                {
                    channel.BasicPublish("", queueName, null, System.Text.Encoding.UTF8.GetBytes($"【{DateTime.Now.ToString("HH:mm:ss.fffffff")}】 【{i}】 hello world"));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                if (channel != null && channel.IsOpen)
                { try { channel.Close(); } catch (Exception ex) { Console.WriteLine(ex.ToString()); } }
                if (conn != null && conn.IsOpen)
                { try { conn.Close(); } catch (Exception ex) { Console.WriteLine(ex.ToString()); } }
            }
        }

        public void Consumer()
        {
            var conn = GetMQConn($"{GetType().Name}_消费者");
            var channel = conn.CreateModel();
            string queueName = $"{GetType().Name}_生产者";

            try
            {
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (sender, args) =>
                {
                    Console.WriteLine(Encoding.UTF8.GetString(args.Body.ToArray()));
                };
                channel.BasicConsume(queueName, true, consumer);
                Console.ReadLine();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                if (channel != null && channel.IsOpen)
                { try { channel.Close(); } catch (Exception ex) { Console.WriteLine(ex.ToString()); } }
                if (conn != null && conn.IsOpen)
                { try { conn.Close(); } catch (Exception ex) { Console.WriteLine(ex.ToString()); } }
            }
        }

        public static IConnection GetMQConn(string connName)
        {
            var factory = new ConnectionFactory();
            factory.UserName = "guest";
            factory.Password = "guest";
            factory.VirtualHost = "/";
            factory.HostName = "127.0.0.1";
            factory.Port = 5672;
            IConnection conn = factory.CreateConnection(connName);
            return conn;
        }
    }
}