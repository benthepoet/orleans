using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Hosting
{
    public static class AwsS3BlobSiloBuilderExtensions
    {
        /// <summary>Configure silo to use azure blob storage for grain storage.</summary>
        public static ISiloHostBuilder AddAwsS3BlobGrainStorage(this ISiloHostBuilder builder, string name, Action<AwsS3BlobStorageOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.AddAwsS3BlobGrainStorage(name, configureOptions));
        }

        /// <summary>Configure silo to use azure blob storage for grain storage.</summary>
        public static ISiloHostBuilder AddAwsS3BlobGrainStorage(this ISiloHostBuilder builder, string name, Action<OptionsBuilder<AwsS3BlobStorageOptions>> configureOptions = null)
        {
            return builder.ConfigureServices(services => services.AddAwsS3BlobGrainStorage(name, configureOptions));
        }

        /// <summary>Configure silo to use azure blob storage for grain storage.</summary>
        public static ISiloBuilder AddAwsS3BlobGrainStorage(this ISiloBuilder builder, string name, Action<AwsS3BlobStorageOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.AddAwsS3BlobGrainStorage(name, configureOptions));
        }

        /// <summary>Configure silo to use azure blob storage for grain storage.</summary>
        public static ISiloBuilder AddAwsS3BlobGrainStorage(this ISiloBuilder builder, string name, Action<OptionsBuilder<AwsS3BlobStorageOptions>> configureOptions = null)
        {
            return builder.ConfigureServices(services => services.AddAwsS3BlobGrainStorage(name, configureOptions));
        }

        /// <summary>Configure silo to use azure blob storage for grain storage.</summary>
        public static IServiceCollection AddAwsS3BlobGrainStorage(this IServiceCollection services, string name, Action<AwsS3BlobStorageOptions> configureOptions)
        {
            return services.AddAwsS3BlobGrainStorage(name, ob => ob.Configure(configureOptions));
        }

        public static IServiceCollection AddAwsS3BlobGrainStorage(this IServiceCollection services, string name,
                    Action<OptionsBuilder<AwsS3BlobStorageOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(services.AddOptions<AwsS3BlobStorageOptions>(name));
            services.AddTransient<IConfigurationValidator>(sp => new AwsS3BlobStorageOptionsValidator(sp.GetRequiredService<IOptionsMonitor<AwsS3BlobStorageOptions>>().Get(name), name));
            services.ConfigureNamedOptionForLogging<AwsS3BlobStorageOptions>(name);
            if (string.Equals(name, ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, StringComparison.Ordinal))
            {
                services.TryAddSingleton(sp => sp.GetServiceByName<IGrainStorage>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));
            }
            return services.AddSingletonNamedService<IGrainStorage>(name, AwsS3BlobStorageFactory.Create)
                           .AddSingletonNamedService(name, (s, n) => (ILifecycleParticipant<ISiloLifecycle>)s.GetRequiredServiceByName<IGrainStorage>(n));
        }

        /// <summary>Configure silo to use azure blob storage as the default grain storage.</summary>
        public static ISiloHostBuilder AddAwsS3BlobGrainStorageAsDefault(this ISiloHostBuilder builder, Action<OptionsBuilder<AwsS3BlobStorageOptions>> configureOptions = null)
        {
            return builder.AddAwsS3BlobGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>Configure silo to use azure blob storage as the default grain storage.</summary>
        public static ISiloBuilder AddAwsS3BlobGrainStorageAsDefault(this ISiloBuilder builder, Action<AwsS3BlobStorageOptions> configureOptions)
        {
            return builder.AddAwsS3BlobGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>Configure silo to use azure blob storage as the default grain storage.</summary>
        public static ISiloBuilder AddAwsS3BlobGrainStorageAsDefault(this ISiloBuilder builder, Action<OptionsBuilder<AwsS3BlobStorageOptions>> configureOptions = null)
        {
            return builder.AddAwsS3BlobGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }        /// <summary>Configure silo to use azure blob storage for grain storage.</summary>

                 /// <summary>Configure silo to use azure blob storage as the default grain storage.</summary>
        public static ISiloHostBuilder AddAwsS3BlobGrainStorageAsDefault(this ISiloHostBuilder builder, Action<AwsS3BlobStorageOptions> configureOptions)
        {
            return builder.AddAwsS3BlobGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>Configure silo to use azure blob storage as the default grain storage.</summary>
        public static IServiceCollection AddAwsS3BlobGrainStorageAsDefault(this IServiceCollection services, Action<AwsS3BlobStorageOptions> configureOptions)
        {
            return services.AddAwsS3BlobGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, ob => ob.Configure(configureOptions));
        }

        /// <summary>Configure silo to use azure blob storage as the default grain storage.</summary>
        public static IServiceCollection AddAwsS3BlobGrainStorageAsDefault(this IServiceCollection services, Action<OptionsBuilder<AwsS3BlobStorageOptions>> configureOptions = null)
        {
            return services.AddAwsS3BlobGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }
    }
}
