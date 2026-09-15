namespace TelemetryHub.Infrastructure.Channels;

using System.Threading.Channels;
using TelemetryHub.Domain.Models;

/// <summary>
/// Abstraction interface for the high-throughput lock-free Telemetry channel.
/// Decouples ASP.NET Core ingestion endpoints from background consumers.
/// </summary>
public interface ITelemetryChannel
{
    /// <summary>
    /// Gets the channel writer used by API ingestion endpoints to enqueue incoming telemetry payloads.
    /// </summary>
    ChannelWriter<TelemetryPayload> Writer { get; }

    /// <summary>
    /// Gets the channel reader used by background worker services to consume telemetry payloads.
    /// </summary>
    ChannelReader<TelemetryPayload> Reader { get; }
}
