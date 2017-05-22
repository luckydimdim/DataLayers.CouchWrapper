using System;
using System.Net;
using System.Threading.Tasks;
using Cmas.Infrastructure.ErrorHandler;
using Microsoft.Extensions.Logging;
using MyCouch;
using MyCouch.Responses;
using Microsoft.Extensions.Options;
using Cmas.Infrastructure.Configuration;

namespace Cmas.DataLayers.Infrastructure
{
    public class CouchWrapper
    {
        private string dbConnectionString;
        private string dbName;
        private readonly ILogger logger;

        public CouchWrapper(string dbConnectionString, string dbName, ILogger logger)
        {
            this.dbConnectionString = dbConnectionString;
            this.dbName = dbName;
            this.logger = logger;
        }

        public CouchWrapper(IServiceProvider serviceProvider, string serviceName)
        { 
            var configuration = serviceProvider.GetConfiguration();
             
            var dbSettings = configuration.GetDbSettings(serviceName);

            dbConnectionString = dbSettings.ConnectionString;
            dbName = dbSettings.Name;

            var _loggerFactory = (ILoggerFactory)serviceProvider.GetService(typeof(ILoggerFactory));
            logger = _loggerFactory.CreateLogger<CouchWrapper>();
        }

        private bool CheckResponse(Response response)
        {
            if (!response.IsSuccess)
            {
                if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    return false;
                }
                else if (response.StatusCode == HttpStatusCode.Conflict)
                {
                    throw new ConflictErrorException();
                }
                else if (response.StatusCode == HttpStatusCode.Forbidden)
                {
                    throw new Exception("Forbidden");
                }

                throw new Exception("Unknown exception");
            }

            return true;
        }

        public async Task<T> GetResponseAsync<T>(Func<MyCouchClient, Task<T>> method) where T : Response
        {
            using (var client = new MyCouchClient(dbConnectionString, dbName))
            {
                var result = await method(client);

                logger.LogInformation(result.ToStringDebugVersion());

                if (!CheckResponse(result))
                {
                    return null;
                }

                return result;
            }
        }

        public async Task<T> GetResponseAsync<T>(Func<MyCouchStore, Task<T>> method) where T : Response
        {
            using (var store = new MyCouchStore(dbConnectionString, dbName))
            {
                var result = await method(store);

                logger.LogInformation(result.ToStringDebugVersion());

                if (!CheckResponse(result))
                {
                    return null;
                }

                return result;
            }
        }

        public async Task<IDocumentHeader> GetHeaderAsync(string id, string rev = null)
        {
            using (var client = new MyCouchClient(dbConnectionString, dbName))
            {
                var result = await client.Documents.HeadAsync(id, rev);

                logger.LogInformation(result.ToStringDebugVersion());

                if (!CheckResponse(result))
                {
                    return null;
                }

                return result;
            }
        }
    }
}