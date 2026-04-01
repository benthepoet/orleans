using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Storage
{
    /// <summary>AWS S3 storage provider for Orleans grain state.</summary>
    public class AwsS3BlobStorage : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
    {
        private readonly IGrainFactory grainFactory;

        private readonly ILogger<AwsS3BlobStorage> logger;

        private readonly string name;

        private readonly AwsS3BlobStorageOptions options;

        private readonly SerializationManager serializationManager;

        private readonly ITypeResolver typeResolver;

        private JsonSerializerSettings? jsonSettings;

        private AmazonS3Client? s3Client;

        /// <summary>Initializes a new instance of the <see cref="AwsS3BlobStorage"/> class.</summary>
        public AwsS3BlobStorage(
            string name,
            SerializationManager serializationManager,
            AwsS3BlobStorageOptions options,
            IGrainFactory grainFactory,
            ITypeResolver typeResolver,
            ILogger<AwsS3BlobStorage> logger)
        {
            this.name = name;
            this.serializationManager = serializationManager;
            this.options = options;
            this.grainFactory = grainFactory;
            this.typeResolver = typeResolver;
            this.logger = logger;
        }

        /// <inheritdoc/>
        public async Task ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            try
            {
                if (logger.IsEnabled(LogLevel.Trace))
                {
                    logger.LogTrace("Clearing: GrainType={GrainType} GrainId={GrainId} ETag={ETag} from Bucket={Bucket}",
                        grainType, grainReference.ToString(), grainState.ETag, options.BucketName);
                }

                grainState.State = Activator.CreateInstance(grainState.Type);
                await this.WriteStateAsync(grainType, grainReference, grainState);
            }
            catch (Exception ex)
            {
                if (options.ThrowOnDeleteError)
                {
                    logger.LogError(ex, "Error clearing state for {GrainType} {GrainId}", grainType, grainReference.ToString());
                    throw;
                }
                else
                {
                    logger.LogWarning(ex, "Error clearing state for {GrainType} {GrainId}", grainType, grainReference.ToString());
                }
            }
        }

        /// <inheritdoc/>
        public void Participate(ISiloLifecycle lifecycle) =>
            lifecycle.Subscribe(OptionFormattingUtilities.Name<AwsS3BlobStorageOptions>(name), options.InitStage, InitializeAsync);

        /// <inheritdoc/>
        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var blobKey = GetBlobKey(grainType, grainReference);

            try
            {
                if (logger.IsEnabled(LogLevel.Trace))
                {
                    logger.LogTrace("Reading: GrainType={GrainType} GrainId={GrainId} ETag={ETag} from Bucket={Bucket}",
                        grainType, grainReference.ToString(), grainState.ETag, options.BucketName);
                }

                var request = new GetObjectRequest
                {
                    BucketName = options.BucketName,
                    Key = blobKey,
                };

                try
                {
                    var objectResponse = await s3Client!.GetObjectAsync(request);

                    await using var dataStream = objectResponse.ResponseStream;
                    using var memoryStream = new MemoryStream();
                    await dataStream.CopyToAsync(memoryStream);
                    memoryStream.Position = 0;

                    grainState.ETag = objectResponse.ETag.Replace("\"", string.Empty);
                    grainState.RecordExists = true;

                    byte[] contents = memoryStream.ToArray();

                    object loadedState;
                    if (contents == null || contents.Length == 0)
                    {
                        if (logger.IsEnabled(LogLevel.Trace))
                            logger.Trace("BlobEmpty reading: GrainType={0} Grainid={1} ETag={2} from BlobKey={3} in BucketName={4}", grainType, grainReference, grainState.ETag, blobKey, request.BucketName);
                        loadedState = null;
                    }
                    else
                    {
                        loadedState = this.ConvertFromStorageFormat(contents, grainState.Type);
                    }

                    grainState.State = loadedState ?? Activator.CreateInstance(grainState.Type);
                    grainState.RecordExists = loadedState is not null;
                }
                catch (AmazonS3Exception s3Exception) when (s3Exception.StatusCode == HttpStatusCode.NotFound)
                {
                    if (logger.IsEnabled(LogLevel.Trace))
                    {
                        logger.LogTrace("Blob not found: GrainType={GrainType} GrainId={GrainId} in Bucket={Bucket}",
                            grainType, grainReference.ToString(), options.BucketName);
                    }

                    grainState.ETag = null;
                    grainState.RecordExists = false;
                }
                catch (Exception exception)
                {
                    logger.LogError(exception, "Failed to read grain state for {GrainType} {GrainId} from bucket {Bucket}",
                            grainType, grainReference.ToString(), options.BucketName);
                    throw;
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error reading state for {GrainType} {GrainId}", grainType, grainReference.ToString());
                throw;
            }
        }

        /// <inheritdoc/>
        public async Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var blobKey = GetBlobKey(grainType, grainReference);

            try
            {
                if (logger.IsEnabled(LogLevel.Trace))
                {
                    logger.LogTrace("Writing: GrainType={GrainType} GrainId={GrainId} ETag={ETag} to Bucket={Bucket}",
                        grainType, grainReference.ToString(), grainState.ETag, options.BucketName);
                }

                var (contents, mimeType) = this.ConvertToStorageFormat(grainState.State);

                var request = new PutObjectRequest
                {
                    BucketName = options.BucketName,
                    ContentType = mimeType,
                    DisablePayloadSigning = options.DisablePayloadSigning,
                    InputStream = new MemoryStream(contents),
                    Key = blobKey
                };

                if (string.IsNullOrEmpty(grainState.ETag))
                {
                    request.IfNoneMatch = "*";
                }
                else
                {
                    request.IfMatch = $"\"{grainState.ETag}\"";
                }

                // Add storage version metadata
                request.Metadata.Add("x-amz-meta-storage-version", "1");

                try
                {
                    var response = await s3Client!.PutObjectAsync(request);
                    grainState.ETag = response.ETag.Replace("\"", string.Empty);
                    grainState.RecordExists = true;
                }
                catch (AmazonS3Exception s3Exception) when (s3Exception.StatusCode == HttpStatusCode.PreconditionFailed || s3Exception.StatusCode == HttpStatusCode.Conflict)
                {
                    throw new InconsistentStateException(
                        $"Inconsistent state for grain {grainReference.ToKeyString()} of type {grainType}",
                        s3Exception);
                }
                catch (Exception e)
                {
                    logger.LogError(e, "Error writing state for {GrainType} {GrainId}", grainType, grainReference.ToKeyString());
                    throw;
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error writing state for {GrainType} {GrainId}", grainType, grainReference.ToKeyString());
                throw;
            }
        }

        private static string GetBlobKey(string grainType, GrainReference grainId) =>
            string.Format("{0}/{1}.json", grainType, grainId.ToKeyString());

        /// <summary>Deserialize from the configured storage format, either binary or JSON.</summary>
        /// <param name="contents">The serialized contents.</param>
        /// <param name="stateType">The state type.</param>
        /// <remarks>
        ///     See: http://msdn.microsoft.com/en-us/library/system.web.script.serialization.javascriptserializer.aspx
        ///     for more on the JSON serializer.
        /// </remarks>
        private object ConvertFromStorageFormat(byte[] contents, Type stateType)
        {
            object result;
            if (options.UseJson)
            {
                var str = Encoding.UTF8.GetString(contents);
                result = JsonConvert.DeserializeObject(str, stateType, jsonSettings);
            }
            else
            {
                result = serializationManager.DeserializeFromByteArray<object>(contents);
            }

            return result;
        }

        /// <summary>Serialize to the configured storage format, either binary or JSON.</summary>
        /// <param name="grainState">The grain state data to be serialized</param>
        /// <remarks>
        ///     See: http://msdn.microsoft.com/en-us/library/system.web.script.serialization.javascriptserializer.aspx
        ///     for more on the JSON serializer.
        /// </remarks>
        private (byte[], string) ConvertToStorageFormat(object grainState)
        {
            byte[] data;
            string mimeType;
            if (options.UseJson)
            {
                data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(grainState, jsonSettings));
                mimeType = "application/json";
            }
            else
            {
                data = serializationManager.SerializeToByteArray(grainState);
                mimeType = "application/octet-stream";
            }

            return (data, mimeType);
        }

        private async Task<bool> DoesBucketExistAsync(string bucketName)
        {
            try
            {
                await s3Client!.GetBucketLocationAsync(new GetBucketLocationRequest { BucketName = bucketName });
                return true;
            }
            catch (AmazonS3Exception e) when (e.StatusCode == HttpStatusCode.NotFound)
            {
                return false;
            }
        }

        private AmazonS3Config GetAmazonS3Config()
        {
            var config = new AmazonS3Config();

            if (!string.IsNullOrEmpty(options.ServiceUrl))
            {
                config.ServiceURL = options.ServiceUrl;
            }

            if (options.ForcePathStyle.HasValue)
            {
                config.ForcePathStyle = options.ForcePathStyle.Value;
            }

            if (options.RequestChecksumCalculation.HasValue)
            {
                config.RequestChecksumCalculation = options.RequestChecksumCalculation.Value
                    ? RequestChecksumCalculation.WHEN_REQUIRED
                    : RequestChecksumCalculation.WHEN_SUPPORTED;
            }

            if (options.ResponseChecksumValidation.HasValue)
            {
                config.RequestChecksumCalculation = options.ResponseChecksumValidation.Value
                    ? RequestChecksumCalculation.WHEN_REQUIRED
                    : RequestChecksumCalculation.WHEN_SUPPORTED;
            }

            return config;
        }

        private async Task InitializeAsync(CancellationToken ct)
        {
            try
            {
                var stopWatch = Stopwatch.StartNew();

                logger.LogInformation("AWS S3 storage initializing: Name={Name}", name);

                jsonSettings = OrleansJsonSerializer.UpdateSerializerSettings(OrleansJsonSerializer.GetDefaultSerializerSettings(typeResolver, grainFactory), options.UseFullAssemblyNames, options.IndentJson, options.TypeNameHandling);
                options.ConfigureJsonSerializerSettings?.Invoke(jsonSettings);

                var config = this.GetAmazonS3Config();

                if (!string.IsNullOrEmpty(options.AccessKey) && !string.IsNullOrEmpty(options.SecretKey))
                {
                    AWSCredentials credentials = new BasicAWSCredentials(options.AccessKey, options.SecretKey);
                    s3Client = new AmazonS3Client(credentials, config);
                }
                else
                {
                    s3Client = new AmazonS3Client(config);
                }

                // Check if bucket exists and create it if needed
                if (!await this.DoesBucketExistAsync(options.BucketName))
                {
                    try
                    {
                        logger.LogInformation("Creating bucket {BucketName}", options.BucketName);
                        await s3Client.PutBucketAsync(new PutBucketRequest { BucketName = options.BucketName }, ct);
                    }
                    catch (AmazonS3Exception e) when (e.StatusCode == HttpStatusCode.Conflict)
                    {
                        // Bucket was created by someone else
                    }
                }

                stopWatch.Stop();
                logger.LogInformation("AWS S3 grain storage initialized in {ElapsedMs}ms: Name={Name}",
                    stopWatch.ElapsedMilliseconds, name);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error initializing AWS S3 grain storage: Name={Name}", name);
                throw;
            }
        }
    }
}
