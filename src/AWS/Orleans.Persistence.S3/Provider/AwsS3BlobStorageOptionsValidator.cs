using Orleans;
using Orleans.Runtime;

namespace Orleans.Storage
{
    /// <summary>Configuration validator for AzureBlobStorageOptions</summary>
    public class AwsS3BlobStorageOptionsValidator : IConfigurationValidator
    {
        private readonly string name;
        private readonly AwsS3BlobStorageOptions options;

        /// <summary>Constructor</summary>
        /// <param name="options">The option to be validated.</param>
        /// <param name="name">The option name to be validated.</param>
        public AwsS3BlobStorageOptionsValidator(AwsS3BlobStorageOptions options, string name)
        {
            this.options = options;
            this.name = name;
        }

        public void ValidateConfiguration()
        {
            if (string.IsNullOrEmpty(options.BucketName))
            {
                throw new OrleansConfigurationException($"AWS S3 grain storage '{name}' requires BucketName to be configured.");
            }
        }
    }
}
