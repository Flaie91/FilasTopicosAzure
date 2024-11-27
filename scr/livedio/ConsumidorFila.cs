using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace livedio
{
    public class ConsumidorFila : BackgroundService
    {
        private readonly ServiceBusClient _client;
        private readonly ServiceBusProcessor _processor;

        public ConsumidorFila()
        {
            // Inicialize o cliente e o processador
            _client = new ServiceBusClient("ConnectionString");
            _processor = _client.CreateProcessor("fila1", new ServiceBusProcessorOptions
            {
                MaxConcurrentCalls = 1, // Processar uma mensagem por vez
                AutoCompleteMessages = false // Controle manual para completar as mensagens
            });

            Console.WriteLine("Iniciando a leitura da fila no ServiceBus...");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // Registrar manipuladores de mensagens e erros
            _processor.ProcessMessageAsync += ProcessarMensagem;
            _processor.ProcessErrorAsync += ProcessarErro;

            // Iniciar o processador
            await _processor.StartProcessingAsync(stoppingToken);

            // Parar o processamento quando o token for cancelado
            stoppingToken.Register(async () =>
            {
                await _processor.StopProcessingAsync();
                await DisposeAsync();
                Console.WriteLine("Finalizando conexão com o Azure Service Bus.");
            });
        }

        public override async Task StopAsync(CancellationToken stoppingToken)
        {
             await _processor.StopProcessingAsync();
            Console.WriteLine("Processamento interrompido.");
        }

        private async Task ProcessarMensagem(ProcessMessageEventArgs args)
        {
            try
            {
                // Obter o corpo da mensagem
                var corpo = args.Message.Body.ToString();
                Console.WriteLine($"[Nova Mensagem Recebida na fila] {corpo}");

                // Completar a mensagem
                await args.CompleteMessageAsync(args.Message);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Erro ao processar mensagem] {ex.Message}");
            }
        }

         private Task ProcessarErro(ProcessErrorEventArgs args)
        {
            Console.WriteLine($"[Erro] {args.Exception.GetType().FullName}: {args.Exception.Message}");
            return Task.CompletedTask;
        }

        public async ValueTask DisposeAsync()
        {
            await _processor.DisposeAsync();
            await _client.DisposeAsync();
        }
    }
}