using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace AX.TestConsole
{
    //fanout模式 发布订阅模式 一种分发机制

    public class MQProgram_PublishSubscribe
    {
        public void Run()
        {
            Console.WriteLine(GetType().FullName);
            Produce();
            Task.Run(() => Consumer("1"));
            Task.Run(() => Consumer("2"));
            Task.Run(() => Consumer("3"));
        }

        public void Produce()
        {
            var conn = MQProgram_Simple.GetMQConn($"{GetType().Name}_生产者");
            var channel = conn.CreateModel();
            string exchangeName = $"{GetType().Name}_exchange_fanout_生产者";

            try
            {
                //声明队列 参数：队列名称，是否持久化，是否独占独立，是否自动删除，附属参数
                channel.QueueDeclare($"{GetType().Name}_生产者_1", false, false, false, null);
                channel.QueueDeclare($"{GetType().Name}_生产者_2", false, false, false, null);
                channel.QueueDeclare($"{GetType().Name}_生产者_3", false, false, false, null);

                //声明交换机 参数：名称，类型 direct|fanout|headers|topic,是否持久化,是否自动删除,附属参数
                channel.ExchangeDeclare(exchangeName, "fanout", false, false, null);

                //绑定关系
                channel.QueueBind($"{GetType().Name}_生产者_1", exchangeName, string.Empty);
                channel.QueueBind($"{GetType().Name}_生产者_2", exchangeName, string.Empty);
                channel.QueueBind($"{GetType().Name}_生产者_3", exchangeName, string.Empty);

                var count = new System.Random().Next(1, 50);
                Console.WriteLine(count);
                for (int i = 0; i < count; i++)
                {
                    channel.BasicPublish(exchangeName, string.Empty, body: Encoding.UTF8.GetBytes($"【{DateTime.Now.ToString("HH:mm:ss.fffffff")}】 【{i}】 hello world"));
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
            if (string.IsNullOrWhiteSpace(queueName))
            { queueName = $"{GetType().Name}_生产者"; }
            else
            { queueName = $"{GetType().Name}_生产者_" + queueName; }

            try
            {
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (sender, args) =>
                {
                    Console.WriteLine($"【{queueName}】 " + Encoding.UTF8.GetString(args.Body.ToArray()));
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