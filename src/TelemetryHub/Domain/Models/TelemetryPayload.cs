namespace TelemetryHub.Domain.Models;

using System.Text.Json.Serialization;

/// <summary>
/// Immutable, zero-allocation Data Transfer Object (DTO) for high-throughput telemetry ingestion.
/// Declared as a <see langword="readonly record struct"/> to guarantee value semantics and prevent
/// Garbage Collection (GC) heap allocations on hot API routes.
/// </summary>
/// <param name="DeviceId">Unique identifier of the sending device/sensor.</param>
/// <param name="Timestamp">UTC timestamp when the measurement was taken.</param>
/// <param name="MetricName">Identifier for the metric (e.g., "cpu_usage", "temperature_celsius").</param>
/// <param name="Value">Numerical reading value.</param>
/// <param name="Tags">Optional key-value pairs representing contextual dimensions (e.g., region, firmware version).</param>
public readonly record struct TelemetryPayload(
    [property: JsonPropertyName("deviceId")] Guid DeviceId,
    [property: JsonPropertyName("timestamp")] DateTimeOffset Timestamp,
    [property: JsonPropertyName("metricName")] string MetricName,
    [property: JsonPropertyName("value")] double Value,
    [property: JsonPropertyName("tags")] IReadOnlyDictionary<string, string>? Tags = null)
{
    /// <summary>
    /// Validates the telemetry payload without allocating heap memory.
    /// </summary>
    /// <returns><see langword="true"/> if payload is valid; otherwise <see langword="false"/>.</returns>
    public bool IsValid() => DeviceId != Guid.Empty 
                             && !string.IsNullOrWhiteSpace(MetricName) 
                             && Timestamp != default;
}
