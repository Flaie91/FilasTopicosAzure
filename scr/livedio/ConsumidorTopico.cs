using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace livedio
{
    public class ConsumidorTopico : BackgroundService
    {
        private readonly ServiceBusProcessor _processor;
        private readonly ILogger<ConsumidorTopico> _log;

        public ConsumidorTopico(ILogger<ConsumidorTopico> log)
        {
            _log = log;

            string connectionString = "ConnectionString";
            string topico = "topico1";
            string assinatura = "assinatura1";

            var client = new ServiceBusClient(connectionString);

            // Criando o processador para o tópico/assinatura
            _processor = client.CreateProcessor(topico, assinatura, new ServiceBusProcessorOptions
            {
                MaxConcurrentCalls = 5,
                AutoCompleteMessages = false // Controle manual para completar mensagens
            });

            Console.WriteLine("Iniciando a leitura do tópico no Service Bus...");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
            _processor.ProcessMessageAsync += ProcessarMensagem;
            _processor.ProcessErrorAsync += ProcessarErro;

            await _processor.StartProcessingAsync(stoppingToken);

            stoppingToken.Register(async () =>
            {
                await _processor.StopProcessingAsync();
                Console.WriteLine("Finalizando conexão com o Azure Service Bus.");
            });
            }
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
                var corpo = args.Message.Body.ToString();

                Console.WriteLine($"[Nova Mensagem Recebida no tópico] {corpo}");

                // Processar ou registrar informações adicionais aqui

                // Confirmação manual de processamento
                await args.CompleteMessageAsync(args.Message);
            }
            catch (Exception ex)
            {
                _log.LogError($"Erro ao processar mensagem: {ex.Message}");
            }
        }

        private Task ProcessarErro(ProcessErrorEventArgs args)
        {
            _log.LogError($"Erro no processamento do tópico: {args.Exception.Message}");
            return Task.CompletedTask;
        }
    }
}