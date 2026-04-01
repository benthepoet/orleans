using System;
using Amazon.S3;
using Newtonsoft.Json;

namespace Orleans.Storage
{
    /// <summary>Configuration options for AWS S3 grain storage.</summary>
    public class AwsS3BlobStorageOptions
    {
        public const string DEFAULT_CONTAINER_NAME = "grainstate";

        public const int DEFAULT_INIT_STAGE = ServiceLifecycleStage.ApplicationServices;

        public string AccessKey { get; set; }

        /// <summary>Bucket name where grain stage is stored</summary>
        public string BucketName { get; set; } = DEFAULT_CONTAINER_NAME;

        /// <summary>
        ///     Options to be used when configuring the AWS S3 client, or <see langword="null"/> to use the default
        ///     options.
        /// </summary>
        public AmazonS3Config ClientOptions { get; set; }

        public Action<JsonSerializerSettings> ConfigureJsonSerializerSettings { get; set; }

        public bool DisablePayloadSigning { get; set; }

        public bool? ForcePathStyle { get; set; }

        public bool IndentJson { get; set; }

        /// <summary>
        ///     Stage of silo lifecycle where storage should be initialized. Storage must be initialized prior to use.
        /// </summary>
        public int InitStage { get; set; } = DEFAULT_INIT_STAGE;

        public bool? RequestChecksumCalculation { get; set; }

        public bool? ResponseChecksumValidation { get; set; }

        public string SecretKey { get; set; }

        public string ServiceUrl { get; set; }

        public bool ThrowOnDeleteError { get; set; }

        public TypeNameHandling? TypeNameHandling { get; set; }

        public bool UseFullAssemblyNames { get; set; }

        public bool UseJson { get; set; }
    }
}
