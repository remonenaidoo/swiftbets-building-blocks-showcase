using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SwiftBets.BuildingBlocks.Core;
using SwiftBets.BuildingBlocks.Resilience;
using SwiftBets.Contracts.Messaging;

namespace SwiftBets.BuildingBlocks.Messaging;

public static class MessagingRegistration
{
    public static IServiceCollection AddKafkaMessaging(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddValidatedOptions<KafkaOptions>(configuration, KafkaOptions.SectionName);
        services.AddSwiftBetsResilience();
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IEventPublisher, KafkaEventPublisher>();
        services.AddHealthChecks().AddCheck<KafkaHealthCheck>("kafka", tags: ["ready"], timeout: TimeSpan.FromSeconds(5));
        return services;
    }

    public static IServiceCollection AddKafkaConsumer<TPayload, THandler>(this IServiceCollection services, string topicBase, string groupId, bool startAtLatest = false, int maxInFlight = 1)
        where TPayload : IEventContract
        where THandler : class, IEventHandler<TPayload>
    {
        services.AddScoped<IEventHandler<TPayload>, THandler>();
        services.AddSingleton<IHostedService>(sp => new KafkaConsumerHost<TPayload>(
            new ConsumerRegistration(topicBase, groupId, startAtLatest, maxInFlight),
            sp.GetRequiredService<IServiceScopeFactory>(),
            sp.GetRequiredService<IEventPublisher>(),
            sp.GetRequiredService<IOptions<KafkaOptions>>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<ILogger<KafkaConsumerHost<TPayload>>>()));
        return services;
    }

    /// <summary>Keeps the latest value per key of a compacted topic in memory; readiness waits until it has caught up.</summary>
    public static IServiceCollection AddCompactedState<TPayload>(this IServiceCollection services, string topicBase)
        where TPayload : IEventContract
    {
        services.AddSingleton(sp => new CompactedStateConsumer<TPayload>(
            topicBase,
            sp.GetRequiredService<IOptions<KafkaOptions>>(),
            sp.GetRequiredService<ILogger<CompactedStateConsumer<TPayload>>>()));
        services.AddSingleton<ICompactedState<TPayload>>(sp => sp.GetRequiredService<CompactedStateConsumer<TPayload>>());
        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<CompactedStateConsumer<TPayload>>());
        services.AddHealthChecks().Add(new HealthCheckRegistration(
            $"state-{topicBase}",
            sp => sp.GetRequiredService<CompactedStateConsumer<TPayload>>(),
            HealthStatus.Unhealthy,
            ["ready"]));
        return services;
    }
}
