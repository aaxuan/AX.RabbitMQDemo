using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace AX.TestConsole
{
    //work 轮询模式
    //一个消费者一条，平均分配
    public class MQProgram_Work_RoundRobin
    {
        public void Run()
        {
            Console.WriteLine(GetType().FullName);
            Task.Run(() => Consumer("1"));
            Task.Run(() => Consumer("2"));
            Produce();
        }

        public void Produce()
        {
            var conn = MQProgram_Simple.GetMQConn($"{GetType().Name}_生产者");
            var channel = conn.CreateModel();
            string exchangeName = string.Empty;

            try
            {
                //声明队列 参数：队列名称，是否持久化，是否独占独立，是否自动删除，附属参数
                channel.QueueDeclare($"{GetType().Name}_生产者", false, false, false, null);
                var count = new System.Random().Next(1, 50);
                Console.WriteLine(count);
                for (int i = 0; i < count; i++)
                {
                    //将通道名称作为 routingKey
                    channel.BasicPublish(string.Empty, $"{GetType().Name}_生产者", body: Encoding.UTF8.GetBytes($"【{DateTime.Now.ToString("HH:mm:ss.fffffff")}】 【{i}】 hello world"));
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

        public void Consumer(string queueName)
        {
            var conn = GetMQConn($"{GetType().Name}_消费者");
            var channel = conn.CreateModel();
            var work = queueName;
            queueName = $"{GetType().Name}_生产者";

            try
            {
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (sender, args) =>
                { Console.WriteLine($"【{queueName}】 【{work}】 " + Encoding.UTF8.GetString(args.Body.ToArray())); };
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

        public IConnection GetMQConn(string connName)
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