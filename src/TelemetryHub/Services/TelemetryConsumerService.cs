namespace TelemetryHub.Services;

using System.Diagnostics.Metrics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TelemetryHub.Domain.Models;
using TelemetryHub.Infrastructure.Channels;
using TelemetryHub.Infrastructure.Storage;

/// <summary>
/// Background worker service responsible for consuming telemetry payloads from the channel pipeline.
/// Designed for high throughput with zero allocations in the main consumption loop.
/// </summary>
/// <param name="channel">The telemetry channel abstraction.</param>
/// <param name="storage">In-memory analytics storage.</param>
/// <param name="logger">Structured logger instance.</param>
public partial class TelemetryConsumerService(
    ITelemetryChannel channel,
    ITelemetryStorage storage,
    ILogger<TelemetryConsumerService> logger) : BackgroundService
{
    private static readonly Meter TelemetryMeter = new("TelemetryHub.Ingestion", "1.0.0");
    private static readonly Counter<long> ItemsConsumedCounter = TelemetryMeter.CreateCounter<long>(
        "telemetry_items_consumed_total",
        "items",
        "Total number of telemetry payloads consumed from the channel pipeline.");

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        LogServiceStarted(logger);

        try
        {
            // ReadAllAsync provides an efficient IAsyncEnumerable loop without unnecessary Task allocations
            await foreach (var payload in channel.Reader.ReadAllAsync(stoppingToken))
            {
                ProcessPayload(in payload);
                ItemsConsumedCounter.Add(1);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal service shutdown requested
            LogServiceStopping(logger);
        }
        catch (Exception ex)
        {
            LogServiceError(logger, ex);
            throw;
        }

        LogServiceStopped(logger);
    }

    /// <summary>
    /// Hot-path payload processor. Stores processed items and calculates analytics.
    /// </summary>
    private void ProcessPayload(in TelemetryPayload payload)
    {
        storage.RecordPayload(in payload);
    }

    #region High-Performance Source-Generated Logging Methods

    [LoggerMessage(EventId = 1, Level = LogLevel.Information, Message = "TelemetryConsumerService started. Listening for incoming channel items...")]
    private static partial void LogServiceStarted(ILogger logger);

    [LoggerMessage(EventId = 2, Level = LogLevel.Information, Message = "TelemetryConsumerService stopping graceful shutdown requested.")]
    private static partial void LogServiceStopping(ILogger logger);

    [LoggerMessage(EventId = 3, Level = LogLevel.Information, Message = "TelemetryConsumerService stopped.")]
    private static partial void LogServiceStopped(ILogger logger);

    [LoggerMessage(EventId = 4, Level = LogLevel.Error, Message = "Unhandled exception occurred in TelemetryConsumerService ingestion loop.")]
    private static partial void LogServiceError(ILogger logger, Exception ex);

    #endregion
}
