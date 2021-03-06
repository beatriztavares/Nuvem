using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Produtor
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);
        static CloudQueue cloudQueue;
        const string MessageText = "Here is a nice message!";

        public void Connect()
        {
            var connectionString = "DefaultEndpointsProtocol=https;AccountName=nuvempuc;AccountKey=5Xl8yKJmsI0Ud0+GiZEqhydILreQIXxOLJqhcrl81xp7PLIpQJK836/cb2DfTmPREDkkdxjNw9HJeuwFCDcF6Q==;EndpointSuffix=core.windows.net";

            CloudStorageAccount cloudStorageAccount;

            if (!CloudStorageAccount.TryParse(connectionString, out cloudStorageAccount))
            {
                Console.WriteLine("Expected connection string 'Azure Storage Account Demo Primary' to be a valid Azure Storage Connection String.");
            }

            var cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();
            cloudQueue = cloudQueueClient.GetQueueReference("queue2");

            // Note: Usually this statement can be executed once during application startup or maybe even never in the application.
            //       A queue in Azure Storage is often considered a persistent item which exists over a long time.
            //       Every time .CreateIfNotExists() is executed a storage transaction and a bit of latency for the call occurs.
            cloudQueue.CreateIfNotExists();
        }

        public void SendMessage()
        {
            var message = new CloudQueueMessage(MessageText);

            cloudQueue.AddMessage(message);
        }

        public override void Run()
        {
            Trace.TraceInformation("Produtor is running");

            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait();
            }
            finally
            {
                this.runCompleteEvent.Set();
            }
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 12;

            // For information on handling configuration changes
            // see the MSDN topic at https://go.microsoft.com/fwlink/?LinkId=166357.

            bool result = base.OnStart();

            Connect();

            Trace.TraceInformation("Produtor has been started");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("Produtor is stopping");

            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("Produtor has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following with your own logic.
            while (!cancellationToken.IsCancellationRequested)
            {
                Trace.TraceInformation("Working");
                SendMessage();
                await Task.Delay(1000);
            }
        }
    }
}
