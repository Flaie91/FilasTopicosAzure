using Azure.Messaging.ServiceBus;
using System;
using System.Text;
using System.Threading.Tasks;

namespace livedio
{
    public class ProdutorFilaAgendamento
    {
        private readonly ServiceBusClient _client; 
        private readonly ServiceBusSender _sender;

        public ProdutorFilaAgendamento()
        {
            _client = new ServiceBusClient("ConnectionString"); 
            _sender = _client.CreateSender("fila1");
        }

        public async Task ProduzirMensagem()
        {
            try 
            { 
                for (int i = 1; i <= 10; i++) 
                { Console.WriteLine($"Enviando mensagem: {i}"); ServiceBusMessage message = new ServiceBusMessage(Encoding.UTF8.GetBytes("Número " + i)) 
                { 
                    ScheduledEnqueueTime = DateTimeOffset.Now.AddMinutes(1) }; 
                    await _sender.SendMessageAsync(message); 
                } 
                Console.WriteLine("Concluido o envio das mensagens"); 
            } 
            catch (Exception ex) 
            { 
                Console.WriteLine($"Erro: {ex.GetType().FullName} | Mensagem: {ex.Message}"); 
            } 
            finally 
            { 
                await _client.DisposeAsync(); 
                Console.WriteLine("Finalizando conexão com ServiceBus");
            }
        }
    }
}