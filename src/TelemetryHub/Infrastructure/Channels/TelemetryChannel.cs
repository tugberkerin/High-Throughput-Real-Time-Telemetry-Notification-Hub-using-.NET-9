namespace TelemetryHub.Infrastructure.Channels;

using System.Threading.Channels;
using Microsoft.Extensions.Options;
using TelemetryHub.Domain.Models;

/// <summary>
/// High-performance thread-safe channel wrapper managing the telemetry ingestion queue.
/// Utilizes <see cref="Channel.CreateBounded{T}(BoundedChannelOptions)"/> for low-latency, backpressure-aware ingestion.
/// </summary>
public sealed class TelemetryChannel : ITelemetryChannel
{
    private readonly Channel<TelemetryPayload> _channel;

    /// <summary>
    /// Initializes a new instance of the <see cref="TelemetryChannel"/> class.
    /// </summary>
    /// <param name="options">Configuration options for channel capacity and backpressure mode.</param>
    public TelemetryChannel(IOptions<TelemetryChannelOptions> options)
    {
        var config = options.Value;

        var channelOptions = new BoundedChannelOptions(config.Capacity)
        {
            FullMode = config.FullMode,
            SingleWriter = false, // ASP.NET Core endpoint handlers call WriteAsync concurrently across multiple HTTP worker threads
            SingleReader = config.SingleReader, // Single consumer worker service reads from the channel
            AllowSynchronousContinuations = config.AllowSynchronousContinuations // Prevents stack-diving on thread pool under high throughput
        };

        _channel = Channel.CreateBounded<TelemetryPayload>(channelOptions);
    }

    /// <inheritdoc />
    public ChannelWriter<TelemetryPayload> Writer => _channel.Writer;

    /// <inheritdoc />
    public ChannelReader<TelemetryPayload> Reader => _channel.Reader;
}
