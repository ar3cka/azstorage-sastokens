using System;
using System.CommandLine;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Queue.Protocol;
using Serilog;

namespace AzStorage.Tools.SasTokens
{
    public enum Command { None, Get, Test }

    public enum StorageService { None, Queue }

    public class Program
    {
        public static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.LiterateConsole(outputTemplate: "{Message}{NewLine}")
                .CreateLogger();

            Log.Information("---------------------------------------------------------------------------------------");
            Log.Information("AZURE storage SAS token generation tool. Version {Version}.", System.Reflection.Assembly.GetEntryAssembly().GetName().Version);
            Log.Information("---------------------------------------------------------------------------------------");
            Log.Information("");

            var command = Command.None;
            var connectionString = string.Empty;
            var accountName = string.Empty;
            var accountKey = string.Empty;
            var storageService = StorageService.None;
            var policyName = string.Empty;
            var queueName = string.Empty;
            var overwritePolicy = false;
            var sasToken = string.Empty;
            var endpoint = string.Empty;
            ArgumentSyntax.Parse(args, syntax => 
            {
                syntax.DefineCommand("get", ref command, Command.Get, "Generate new or get existing SAS token for specified storage entity");
                syntax.DefineOption("a|account", ref accountName, "Storage account name");
                syntax.DefineOption("k|key", ref accountKey, "Storage account key");
                syntax.DefineOption("s|service", ref storageService, value => (StorageService)Enum.Parse(typeof(StorageService), value, true), "Storage service { queue | blob | table }");
                syntax.DefineOption("p|policy", ref policyName, "Persitent SAS token policy name");
                syntax.DefineOption("q|queue", ref queueName, "Queue name");
                syntax.DefineOption("f|force", ref overwritePolicy, "Overwrite existing policy");
                
                syntax.DefineCommand("test", ref command, Command.Test, "Test specified SAS token");
                syntax.DefineOption("s|service", ref storageService, value => (StorageService)Enum.Parse(typeof(StorageService), value, true), "Storage service { queue | blob | table }");
                syntax.DefineOption("t|token", ref sasToken, "SAS token");
                syntax.DefineOption("e|endpoint", ref endpoint, "Service endpoint uri");
            });

            switch (command)
            {
                case Command.Get:
                    GetSasToken(
                        accountName: accountName,
                        accountKey: accountKey,
                        queueName: queueName,
                        policyName: policyName,
                        overwritePolicy: overwritePolicy);
                    break;

                case Command.Test:
                    TestSasToken(
                        endpoint: new Uri(endpoint),
                        sasToken: sasToken);
                    break;
            }

            Log.Information("");
            Log.Information("Done.");
        }

        private static bool CanAddNewPolicy(QueuePermissions permissions, string policyName, bool overwritePolicy)
        {
            if (permissions.SharedAccessPolicies.ContainsKey(policyName))
            {
                if (!overwritePolicy)
                {
                    Log.Information("Policy {PolicyName} already exists. Reusing existing token...", policyName);
                    return false;
                }

                Log.Information("Policy {PolicyName} already exists. Overwriting with new token...", policyName);
                permissions.SharedAccessPolicies.Remove(policyName);
            }

            return true;
        }

        private static void AddNewPolicyToPermissions(QueuePermissions permissions, string policyName)
        {
            var policy = new SharedAccessQueuePolicy();
            policy.Permissions = SharedAccessQueuePermissions.ProcessMessages;
            policy.SharedAccessExpiryTime = DateTimeOffset.MaxValue;
            permissions.SharedAccessPolicies.Add(policyName, policy);
        }

        private static string GeneratePersistentToken(
            CloudQueue queue, 
            string queueName, 
            string policyName,
            bool overwritePolicy)
        {
            var queuePermissions = queue.GetPermissionsAsync().Result;
            if (CanAddNewPolicy(queuePermissions, policyName, overwritePolicy))
            {
                AddNewPolicyToPermissions(queuePermissions, policyName);
                queue.SetPermissionsAsync(queuePermissions).Wait();
            }
            
            return queue.GetSharedAccessSignature(null, policyName);
        }

        private static string GenerateAdhocToken(
            CloudQueue queue)
        {
            var policy = new SharedAccessQueuePolicy();
            policy.Permissions = SharedAccessQueuePermissions.ProcessMessages;
            policy.SharedAccessExpiryTime = DateTimeOffset.MaxValue;
            
            return queue.GetSharedAccessSignature(policy);
        }

        private static void GetSasToken(
            string accountName, 
            string accountKey, 
            string queueName, 
            string policyName, 
            bool overwritePolicy)
        {
            var storageCredentials = new StorageCredentials(accountName, accountKey);
            var account = new CloudStorageAccount(storageCredentials, null, true);
            var queueClient = account.CreateCloudQueueClient();
            var queue = queueClient.GetQueueReference(queueName);
            var sasToken = string.Empty;

            if (string.IsNullOrEmpty(policyName))
            {
                sasToken = GenerateAdhocToken(
                    queue: queue);
            }
            else 
            {
                Log.Information("Generating SAS token for {QueueName} queue with policy {PolicyName}...", queueName, policyName);

                sasToken = GeneratePersistentToken(
                    queue: queue,
                    queueName: queueName,
                    policyName: policyName,
                    overwritePolicy: overwritePolicy);
            }
            
            Log.Information("Queue address   : {QueueAddress}.", queue.Uri);
            Log.Information("Queue SAS token : {SASToken}.", sasToken);
        }

        public static void TestSasToken(Uri endpoint, string sasToken)
        {
            var storageCredentials = new StorageCredentials(sasToken);
            var cloudQueue = new CloudQueue(endpoint, storageCredentials);

            Log.Information("Checking access to the {QueueName} queue...", cloudQueue.Name);
            cloudQueue.GetMessageAsync().Wait();

            Log.Information("SAS token is OK");
        }
    }
}