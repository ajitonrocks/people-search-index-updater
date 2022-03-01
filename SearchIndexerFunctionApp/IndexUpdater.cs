using Azure;
using Azure.Identity;
using Azure.Search.Documents;
using Azure.Search.Documents.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SearchIndexerFunctionApp
{
    public class IndexUpdater
    {
        private ILogger _logger;
        private GraphServiceClient _graphServiceClient;
        private SearchClient _searchClient;

        [FunctionName("IndexUpdater")]
        public async Task Run([TimerTrigger("* 0 * * * *")]TimerInfo myTimer, ILogger log)
        {
            // Reading configuration values
            _logger = log;
            _logger.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
            var scopes = new[] { "https://graph.microsoft.com/.default" };
            string tenantId = Environment.GetEnvironmentVariable("TenantID");
            string clientId = Environment.GetEnvironmentVariable("ApplicationID");
            string clientSecret = Environment.GetEnvironmentVariable("ApplicationSecret");
            Uri searchServiceUri = new Uri(Environment.GetEnvironmentVariable("SearchServiceUri"));
            string indexName = Environment.GetEnvironmentVariable("SearchIndexName");
            string adminApiKey = Environment.GetEnvironmentVariable("SearchServiceAdminApiKey");

            //Making credentials and Loading people data from Graph API
            var options = new TokenCredentialOptions
            {
                AuthorityHost = AzureAuthorityHosts.AzurePublicCloud
            };
            var clientSecretCredential = new ClientSecretCredential(tenantId, clientId, clientSecret, options);
            _graphServiceClient = new GraphServiceClient(clientSecretCredential, scopes);

            // Create a client
            AzureKeyCredential credential = new AzureKeyCredential(adminApiKey);
            _searchClient  = new SearchClient(searchServiceUri, indexName, credential);

            List<People> peopleData = new List<People>();
            await LoadUsersFromAADAsync(peopleData);
        }


        private async Task LoadUsersFromAADAsync(List<People> peopleData) 
        {
            IGraphServiceUsersCollectionPage usersPage = await _graphServiceClient.Users.Request().Header("ConsistencyLevel", "eventual")
                    .Select("Id,displayName,department,jobTitle,mail,mobilePhone,country,employeeId,businessPhones")
                    .Top(100).GetAsync();
            var pageIterator = PageIterator<User>
           .CreatePageIterator(_graphServiceClient, usersPage, (usr) =>
           {
               peopleData.Add(new People
               {
                   Id = usr.Id,
                   displayName = usr.DisplayName,
                   department = usr.Department,
                   jobTitle = usr.JobTitle,
                   mail = usr.Mail,
                   mobilePhone = usr.MobilePhone,
                   employeeId = usr.EmployeeId,
                   country = usr.Country,
                   businessPhones = usr.BusinessPhones.FirstOrDefault(),
                   picture = "https://onrocks.sharepoint.com/ProfilePics/" + usr.Id + ".jpg"
               });
               return true;
           },
               (req) =>
               {
                    // Re-add the header to subsequent requests
                    req.Header("ConsistencyLevel", "eventual");
                   return req;
               }
           );
            await pageIterator.IterateAsync();
        }
        private async Task<IndexDocumentsResult> ExponentialBackoffAsync(List<People> PeopleList, int id)
        {
            // Create batch of documents for indexing
            var batch = IndexDocumentsBatch.Upload(PeopleList);

            // Create an object to hold the result
            IndexDocumentsResult result = null;

            // Define parameters for exponential backoff
            int attempts = 0;
            TimeSpan delay = delay = TimeSpan.FromSeconds(2);
            int maxRetryAttempts = 5;

            // Implement exponential backoff
            do
            {
                try
                {
                    attempts++;
                    result = await _searchClient.IndexDocumentsAsync(batch).ConfigureAwait(false);

                    var failedDocuments = result.Results.Where(r => r.Succeeded != true).ToList();

                    // handle partial failure
                    if (failedDocuments.Count > 0)
                    {

                        if (attempts == maxRetryAttempts)
                        {
                            _logger.LogInformation("[MAX RETRIES HIT] - Giving up on the batch starting at {0}", id);
                            break;
                        }
                        else
                        {
                            _logger.LogInformation("[Batch starting at doc {0} had partial failure]", id);
                            //_logger.WriteLine("[Attempt: {0} of {1} Failed]", attempts, maxRetryAttempts);
                            _logger.LogInformation("[Retrying {0} failed documents] \n", failedDocuments.Count);

                            // creating a batch of failed documents to retry
                            var failedDocumentKeys = failedDocuments.Select(doc => doc.Key).ToList();
                            PeopleList = PeopleList.Where(h => failedDocumentKeys.Contains(h.Id)).ToList();
                            batch = IndexDocumentsBatch.Upload(PeopleList);

                            Task.Delay(delay).Wait();
                            delay = delay * 2;
                            continue;
                        }
                    }
                    return result;
                }
                catch (RequestFailedException ex)
                {
                    _logger.LogError("[Batch starting at doc {0} failed]. Error Message {1}", id, ex.Message);
                    //_logger.LogError("[Attempt: {0} of {1} Failed] - Error: {2} \n", attempts, maxRetryAttempts, ex.Message);
                    _logger.LogError("[Retrying entire batch] \n");

                    if (attempts == maxRetryAttempts)
                    {
                        _logger.LogError("[MAX RETRIES HIT] - Giving up on the batch starting at {0}", id);
                        break;
                    }

                    Task.Delay(delay).Wait();
                    delay = delay * 2;
                }
            } while (true);

            return null;
        }
        private async Task IndexPeopleDataAsync(List<People> PeopleList, int batchSize, int numThreads)
        {
            int numDocs = PeopleList.Count;
            _logger.LogInformation("Uploading {0} documents...\n", numDocs.ToString());

            DateTime startTime = DateTime.Now;
            _logger.LogInformation("Started at: {0} \n", startTime);
            _logger.LogInformation("Creating {0} threads...\n", numThreads);

            // Creating a list to hold active tasks
            List<Task<IndexDocumentsResult>> uploadTasks = new List<Task<IndexDocumentsResult>>();

            for (int i = 0; i < numDocs; i += batchSize)
            {
                int chunkSize = ((PeopleList.Count() / batchSize) == (i / batchSize)) ? (PeopleList.Count() - i) : batchSize;
                List<People> peopleBatch = PeopleList.GetRange(i, chunkSize);
                var task = ExponentialBackoffAsync(peopleBatch, i);
                uploadTasks.Add(task);
                _logger.LogInformation("Sending a batch of {0} docs starting with doc {1}...\n", chunkSize, i);

                // Checking if we've hit the specified number of threads
                if (uploadTasks.Count >= numThreads)
                {
                    Task<IndexDocumentsResult> firstTaskFinished = await Task.WhenAny(uploadTasks);
                    _logger.LogInformation("Finished a thread, kicking off another...");
                    uploadTasks.Remove(firstTaskFinished);
                }
            }

            // waiting for remaining results to finish
            await Task.WhenAll(uploadTasks);

            DateTime endTime = DateTime.Now;

            TimeSpan runningTime = endTime - startTime;
            _logger.LogInformation("\nEnded at: {0} \n", endTime);
            _logger.LogInformation("Upload time total: {0}", runningTime);

            double timePerBatch = Math.Round(runningTime.TotalMilliseconds / (numDocs / batchSize), 4);
            _logger.LogInformation("Upload time per batch: {0} ms", timePerBatch);

            double timePerDoc = Math.Round(runningTime.TotalMilliseconds / numDocs, 4);
            _logger.LogInformation("Upload time per document: {0} ms \n", timePerDoc);
        }
    }
}
