using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Storage
{
    /// <summary>Factory for creating AWS S3 grain storage instances.</summary>
    internal static class AwsS3BlobStorageFactory
    {
        /// <summary>Creates a new instance of the AWS S3 grain storage.</summary>
        /// <param name="services">The service provider.</param>
        /// <param name="name">The name of the storage provider.</param>
        /// <returns>An initialized AWS S3 grain storage.</returns>
        public static AwsS3BlobStorage Create(IServiceProvider services, string name)
        {
            var optionsMonitor = services.GetRequiredService<IOptionsMonitor<AwsS3BlobStorageOptions>>();
            var loggerFactory = services.GetRequiredService<ILoggerFactory>();

            var options = optionsMonitor.Get(name);
            var logger = loggerFactory.CreateLogger<AwsS3BlobStorage>();

            return ActivatorUtilities.CreateInstance<AwsS3BlobStorage>(services, name, options, logger);
        }
    }
}
