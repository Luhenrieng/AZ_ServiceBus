// See https://aka.ms/new-console-template for more information


using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Amqp.Framing;

public static class Program
{

    [STAThread]
    static void Main()
    {
        var sbService = new ServiceBusService();

        //sbService.SendMessage("Hello world2!");

        sbService.SendMessageToTopic("Hello Topic!");

        //sbService.ReceiveMessage().Wait();

        sbService.ReceiveMessageFromSubscription().Wait();
    }
}

public class ServiceBusService
{
    const string queueName = "XXX";
    const string topicName = "XXX";

    public void SendMessage(string msg)
    {

        ServiceBusClient client = new("XXX");

        ServiceBusSender sender = client.CreateSender(queueName);

        ServiceBusMessage message = new(msg);

        Console.WriteLine($"Message send: {message.MessageId}");

        sender.SendMessageAsync(message).Wait();
    }

    public void SendMessageToTopic(string msg)
    {

        ServiceBusClient client = new("XXX");

        ServiceBusSender sender = client.CreateSender(topicName);

        ServiceBusMessage message = new(msg);

        Console.WriteLine($"Message send: {message.MessageId}");

        sender.SendMessageAsync(message).Wait();
    }

    public async Task ReceiveMessage()
    {

        ServiceBusClient client = new("XXX");

        ServiceBusReceiver receiver = client.CreateReceiver(queueName);

        for (int i = 0; i < 50; i++)
        {
            ServiceBusReceivedMessage receivedMessage = await receiver.ReceiveMessageAsync();

            if (receivedMessage?.Body != null)
            {
                string body = receivedMessage.Body.ToString();

                await receiver.CompleteMessageAsync(receivedMessage);

                Console.WriteLine($"Message received: {body}");
            }

        }
    }

    internal async Task ReceiveMessageFromSubscription()
    {
        ServiceBusClient client = new("XXX");
        ServiceBusProcessor processor = client.CreateProcessor("testtopic1", "testsubscription2", new ServiceBusProcessorOptions());

        try
        {
            processor.ProcessMessageAsync += MessageHandler;

            processor.ProcessErrorAsync += ErrorHandler;

            await processor.StartProcessingAsync();

            Thread.Sleep(10000);

            await processor.StopProcessingAsync();
        }
        finally
        {
            await processor.DisposeAsync();
            await client.DisposeAsync();
        }
    }

    async Task MessageHandler(ProcessMessageEventArgs args)
    {
        string body = args.Message.Body.ToString();
        Console.WriteLine($"Received: {body} from subscription.");

        // complete the message. messages is deleted from the subscription. 
        await args.CompleteMessageAsync(args.Message);
    }

    // handle any errors when receiving messages
    Task ErrorHandler(ProcessErrorEventArgs args)
    {
        Console.WriteLine(args.Exception.ToString());
        return Task.CompletedTask;
    }
}
