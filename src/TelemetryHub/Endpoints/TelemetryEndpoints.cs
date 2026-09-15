namespace TelemetryHub.Endpoints;

using System.Diagnostics.Metrics;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using TelemetryHub.Domain.Models;
using TelemetryHub.Infrastructure.Channels;
using TelemetryHub.Infrastructure.Storage;

/// <summary>
/// Defines high-performance Minimal API endpoints for telemetry ingestion, live results, and analytics.
/// </summary>
public static class TelemetryEndpoints
{
    private static long _totalIngestedCount;
    private static readonly DateTimeOffset StartTime = DateTimeOffset.UtcNow;
    private static readonly Meter TelemetryIngestionMeter = new("TelemetryHub.Api", "1.0.0");
    private static readonly Counter<long> IngestedItemsCounter = TelemetryIngestionMeter.CreateCounter<long>(
        "telemetry_items_ingested_total",
        "items",
        "Total number of telemetry payloads received and written to the ingestion channel.");

    /// <summary>
    /// Registers telemetry ingestion and reporting endpoints into the HTTP request pipeline.
    /// </summary>
    /// <param name="app">The endpoint route builder.</param>
    /// <returns>The route builder for chaining configuration.</returns>
    public static IEndpointRouteBuilder MapTelemetryEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/v1/telemetry")
                       .WithTags("Telemetry Ingestion");

        // Single telemetry ingestion endpoint
        group.MapPost("/", async (
            TelemetryPayload payload,
            ITelemetryChannel channel,
            CancellationToken ct) =>
        {
            if (!payload.IsValid())
            {
                return Results.BadRequest(new { error = "Invalid telemetry payload: DeviceId, MetricName, and Timestamp are required." });
            }

            // High-throughput fast path: TryWrite avoids task allocation if buffer has space
            if (!channel.Writer.TryWrite(payload))
            {
                // Channel full or waiting backpressure: fall back to async write with cancellation token
                await channel.Writer.WriteAsync(payload, ct);
            }

            Interlocked.Increment(ref _totalIngestedCount);
            IngestedItemsCounter.Add(1);
            return Results.Accepted(uri: null, value: payload);
        })
        .WithName("IngestSingleTelemetry")
        .Produces<TelemetryPayload>(StatusCodes.Status202Accepted)
        .Produces(StatusCodes.Status400BadRequest);

        // Batch telemetry ingestion endpoint
        group.MapPost("/batch", async (
            TelemetryPayload[] batchPayloads,
            ITelemetryChannel channel,
            CancellationToken ct) =>
        {
            if (batchPayloads is null || batchPayloads.Length == 0)
            {
                return Results.BadRequest(new { error = "Batch payload collection cannot be empty." });
            }

            int enqueuedCount = 0;
            for (int i = 0; i < batchPayloads.Length; i++)
            {
                ref readonly var item = ref batchPayloads[i];
                if (!item.IsValid()) continue;

                if (!channel.Writer.TryWrite(item))
                {
                    await channel.Writer.WriteAsync(item, ct);
                }
                enqueuedCount++;
            }

            Interlocked.Add(ref _totalIngestedCount, enqueuedCount);
            IngestedItemsCounter.Add(enqueuedCount);
            return Results.Accepted(uri: null, value: new { count = enqueuedCount });
        })
        .WithName("IngestBatchTelemetry")
        .Produces(StatusCodes.Status202Accepted)
        .Produces(StatusCodes.Status400BadRequest);

        // Dashboard Live Stats Endpoint
        group.MapGet("/stats", () =>
        {
            return Results.Ok(new
            {
                status = "Healthy",
                totalIngested = Interlocked.Read(ref _totalIngestedCount),
                channelCapacity = 10000,
                uptimeSeconds = (long)(DateTimeOffset.UtcNow - StartTime).TotalSeconds,
                framework = ".NET 9 (C# 13)",
                architecture = "System.Threading.Channels (BoundedChannel)"
            });
        })
        .WithName("GetTelemetryStats")
        .Produces(StatusCodes.Status200OK);

        // Endpoint to fetch recent processed telemetry payloads (Result View)
        group.MapGet("/recent", (ITelemetryStorage storage, int? count) =>
        {
            var items = storage.GetRecentPayloads(count ?? 50);
            return Results.Ok(items);
        })
        .WithName("GetRecentTelemetry")
        .Produces<IReadOnlyList<TelemetryPayload>>(StatusCodes.Status200OK);

        // Endpoint to fetch real-time calculated analytics per metric
        group.MapGet("/analytics", (ITelemetryStorage storage) =>
        {
            var analytics = storage.GetAnalytics();
            return Results.Ok(analytics);
        })
        .WithName("GetTelemetryAnalytics")
        .Produces<IReadOnlyList<MetricAnalytics>>(StatusCodes.Status200OK);

        return app;
    }
}
