using System.Collections.Concurrent;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SwiftBets.BuildingBlocks.Messaging;
using SwiftBets.BuildingBlocks.Testing;
using SwiftBets.Contracts.Messaging;

namespace SwiftBets.BuildingBlocks.IntegrationTests;

public sealed class KafkaConsumerHostTests(RedpandaFixture redpanda)
{
    private const string TopicBase = "test.thing-happened.v1";

    [Fact]
    public async Task Poison_message_is_dead_lettered_and_the_partition_keeps_flowing()
    {
        await using var harness = await Harness.StartAsync(redpanda, failuresBeforeSuccess: 0);

        await harness.PublishAsync("good-1");
        await harness.PublishRawAsync("{not json"u8.ToArray());
        await harness.PublishAsync("good-2");

        await harness.WaitForHandledAsync(2);
        harness.Handled.ShouldBe(["good-1", "good-2"]);
        var dead = harness.ReadDeadLetter();
        Encoding.UTF8.GetString(dead.Message.Headers.GetLastBytes(MessageHeaders.DeadLetterReason)).ShouldStartWith("deserialization");
    }

    [Fact]
    public async Task Transient_failure_is_redelivered_not_dead_lettered()
    {
        await using var harness = await Harness.StartAsync(redpanda, failuresBeforeSuccess: 1);

        await harness.PublishAsync("flaky");

        await harness.WaitForHandledAsync(1);
        harness.Attempts.ShouldBe(2);
        harness.DeadLetterIsEmpty().ShouldBeTrue();
    }

    [Fact]
    public async Task Unpublishable_dead_letter_keeps_the_consumer_alive_and_retries()
    {
        await using var harness = await Harness.StartAsync(redpanda, failuresBeforeSuccess: 0, failDeadLetters: true);

        await harness.PublishRawAsync("{not json"u8.ToArray());
        await harness.PublishAsync("after-poison");
        await Task.Delay(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);

        harness.IsRunning.ShouldBeTrue();
        harness.Handled.ShouldBeEmpty();
    }

    [Fact]
    public async Task In_flight_mode_lets_other_keys_pass_a_slow_message_and_keeps_each_key_in_order()
    {
        await using var harness = await Harness.StartAsync(redpanda, failuresBeforeSuccess: 0, maxInFlight: 8);

        await harness.PublishAsync("slow-1", "k1");
        await harness.PublishAsync("fast", "k2");
        await harness.PublishAsync("after-slow", "k1");

        await harness.WaitForHandledAsync(3);
        harness.Handled.ShouldBe(["fast", "slow-1", "after-slow"]);
    }

    [Fact]
    public async Task In_flight_mode_redelivers_a_failed_message_and_holds_its_key_behind_it()
    {
        await using var harness = await Harness.StartAsync(redpanda, failuresBeforeSuccess: 1, maxInFlight: 8);

        await harness.PublishAsync("flaky", "k1");
        await harness.PublishAsync("follower", "k1");

        await harness.WaitForHandledAsync(2);
        harness.Handled.ShouldBe(["flaky", "follower"]);
        harness.DeadLetterIsEmpty().ShouldBeTrue();
    }

    private sealed class Harness : IAsyncDisposable
    {
        private readonly IHost _host;
        private readonly KafkaEventPublisher _publisher;
        private readonly RecordingHandler.State _state;
        private readonly string _topic;
        private readonly string _bootstrap;

        private Harness(IHost host, KafkaEventPublisher publisher, RecordingHandler.State state, string topic, string bootstrap)
        {
            _host = host;
            _publisher = publisher;
            _state = state;
            _topic = topic;
            _bootstrap = bootstrap;
        }

        public IReadOnlyList<string> Handled => [.. _state.Handled];

        public int Attempts => _state.Attempts;

        public bool IsRunning => !_host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.IsCancellationRequested;

        public static async Task<Harness> StartAsync(RedpandaFixture redpanda, int failuresBeforeSuccess, bool failDeadLetters = false, int maxInFlight = 1)
        {
            var environment = Kafka.UniqueEnvironment();
            var topic = TopicName.For(TopicBase, environment);
            await redpanda.CreateTopicsAsync(1, topic.Value, topic.DeadLetter().Value);
            var options = Kafka.Options(redpanda.BootstrapServers, environment);
            var publisher = Kafka.Publisher(options);
            var state = new RecordingHandler.State { FailuresBeforeSuccess = failuresBeforeSuccess };
            options.Value.MaxTransientBackoffSeconds = 1;

            var host = Host.CreateDefaultBuilder()
                .ConfigureServices(services =>
                {
                    services.AddSingleton(state);
                    services.AddSingleton(TimeProvider.System);
                    services.AddSingleton<IEventPublisher>(failDeadLetters ? new DeadLetterRefusingPublisher(publisher) : publisher);
                    services.AddSingleton(options);
                    services.AddKafkaConsumer<TestEvent, RecordingHandler>(TopicBase, "group-" + environment, maxInFlight: maxInFlight);
                })
                .Build();
            await host.StartAsync();
            return new Harness(host, publisher, state, topic.Value, redpanda.BootstrapServers);
        }

        public Task PublishAsync(string name, string key = "k") =>
            _publisher.PublishAsync(TopicBase, key, EventEnvelope<TestEvent>.Create(new TestEvent(name, 1), DateTimeOffset.UtcNow, "corr"), CancellationToken.None);

        public Task PublishRawAsync(byte[] value) =>
            _publisher.PublishRawAsync(new OutgoingMessage(_topic, "k", value, new Dictionary<string, string>()), CancellationToken.None);

        public async Task WaitForHandledAsync(int count)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            while (_state.Handled.Count < count)
            {
                await Task.Delay(100, timeout.Token);
            }
        }

        public ConsumeResult<string, byte[]> ReadDeadLetter()
        {
            using var consumer = DeadLetterConsumer();
            return consumer.Consume(TimeSpan.FromSeconds(30)) ?? throw new InvalidOperationException("No dead letter arrived.");
        }

        public bool DeadLetterIsEmpty()
        {
            using var consumer = DeadLetterConsumer();
            return consumer.Consume(TimeSpan.FromSeconds(3)) is null;
        }

        public async ValueTask DisposeAsync()
        {
            await _host.StopAsync();
            _host.Dispose();
            _publisher.Dispose();
        }

        private IConsumer<string, byte[]> DeadLetterConsumer()
        {
            var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
            {
                BootstrapServers = _bootstrap,
                GroupId = "dlq-reader-" + Guid.NewGuid().ToString("N"),
                AutoOffsetReset = AutoOffsetReset.Earliest,
            }).Build();
            consumer.Subscribe(_topic + ".dlq");
            return consumer;
        }
    }

    private sealed class DeadLetterRefusingPublisher(IEventPublisher inner) : IEventPublisher
    {
        public Task PublishAsync<TPayload>(string topicBase, string key, EventEnvelope<TPayload> envelope, CancellationToken cancellationToken)
            where TPayload : IEventContract => inner.PublishAsync(topicBase, key, envelope, cancellationToken);

        public Task PublishRawAsync(OutgoingMessage message, CancellationToken cancellationToken) =>
            message.Topic.EndsWith(".dlq", StringComparison.Ordinal)
                ? throw new TimeoutException("dead-letter topic unavailable")
                : inner.PublishRawAsync(message, cancellationToken);
    }

    private sealed class RecordingHandler(RecordingHandler.State state) : IEventHandler<TestEvent>
    {
        public async Task HandleAsync(ConsumedEvent<TestEvent> message, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref state.AttemptCounter) <= state.FailuresBeforeSuccess)
            {
                throw new TimeoutException("database briefly unavailable");
            }

            if (message.Envelope.Payload.Name.StartsWith("slow", StringComparison.Ordinal))
            {
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }

            state.Handled.Enqueue(message.Envelope.Payload.Name);
        }

        public sealed class State
        {
            public int AttemptCounter;

            public int FailuresBeforeSuccess { get; init; }

            public int Attempts => AttemptCounter;

            public ConcurrentQueue<string> Handled { get; } = new();
        }
    }
}
