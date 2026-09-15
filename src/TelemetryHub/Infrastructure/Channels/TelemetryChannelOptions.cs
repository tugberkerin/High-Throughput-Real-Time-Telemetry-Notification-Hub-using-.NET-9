namespace TelemetryHub.Infrastructure.Channels;

using System.Threading.Channels;

/// <summary>
/// Strongly-typed configuration options for the high-throughput telemetry channel.
/// </summary>
public sealed class TelemetryChannelOptions
{
    /// <summary>
    /// Configuration section name in appsettings.json.
    /// </summary>
    public const string SectionName = "TelemetryChannel";

    /// <summary>
    /// Gets or sets the maximum buffer capacity of the bounded channel. Defaults to 10,000.
    /// </summary>
    public int Capacity { get; init; } = 10_000;

    /// <summary>
    /// Gets or sets the behavior when writing to a full channel. Defaults to <see cref="BoundedChannelFullMode.Wait"/>.
    /// </summary>
    public BoundedChannelFullMode FullMode { get; init; } = BoundedChannelFullMode.Wait;

    /// <summary>
    /// Gets or sets whether there is guaranteed to only ever be a single reader. Defaults to <see langword="true"/>.
    /// </summary>
    public bool SingleReader { get; init; } = true;

    /// <summary>
    /// Gets or sets whether writers to the channel may perform operations synchronously.
    /// Set to <see langword="false"/> to avoid thread pool starvation and unhandled stack dives under high throughput.
    /// </summary>
    public bool AllowSynchronousContinuations { get; init; } = false;
}
