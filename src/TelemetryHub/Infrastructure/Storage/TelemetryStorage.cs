namespace TelemetryHub.Infrastructure.Storage;

using System.Collections.Concurrent;
using TelemetryHub.Domain.Models;

/// <summary>
/// Model representing calculated analytical insights for a metric.
/// </summary>
public record MetricAnalytics(
    string MetricName,
    int Count,
    double MinValue,
    double MaxValue,
    double AverageValue,
    string Status);

/// <summary>
/// Interface for telemetry analytics storage.
/// </summary>
public interface ITelemetryStorage
{
    void RecordPayload(in TelemetryPayload payload);
    IReadOnlyList<TelemetryPayload> GetRecentPayloads(int count = 50);
    IReadOnlyList<MetricAnalytics> GetAnalytics();
}

/// <summary>
/// Thread-safe in-memory storage holding recent telemetry items and real-time analytical calculations.
/// </summary>
public sealed class TelemetryStorage : ITelemetryStorage
{
    private readonly ConcurrentQueue<TelemetryPayload> _recentItems = new();
    private readonly ConcurrentDictionary<string, List<double>> _metricValues = new();
    private const int MaxCapacity = 50;

    public void RecordPayload(in TelemetryPayload payload)
    {
        _recentItems.Enqueue(payload);
        while (_recentItems.Count > MaxCapacity)
        {
            _recentItems.TryDequeue(out _);
        }

        double val = payload.Value;
        string metricName = payload.MetricName;

        _metricValues.AddOrUpdate(
            metricName,
            [val],
            (_, list) =>
            {
                lock (list)
                {
                    list.Add(val);
                    if (list.Count > 100) list.RemoveAt(0);
                }
                return list;
            });
    }

    public IReadOnlyList<TelemetryPayload> GetRecentPayloads(int count = 50)
    {
        return _recentItems.ToArray().Reverse().Take(count).ToList();
    }

    public IReadOnlyList<MetricAnalytics> GetAnalytics()
    {
        var result = new List<MetricAnalytics>();

        foreach (var (metricName, list) in _metricValues)
        {
            double[] snapshot;
            lock (list)
            {
                snapshot = list.ToArray();
            }

            if (snapshot.Length == 0) continue;

            double min = snapshot.Min();
            double max = snapshot.Max();
            double avg = Math.Round(snapshot.Average(), 2);
            
            string status = max > 85.0 ? "🚨 CRITICAL ALARM" : (max > 70.0 ? "⚠️ WARNING" : "🟢 NORMAL");

            result.Add(new MetricAnalytics(metricName, snapshot.Length, min, max, avg, status));
        }

        return result;
    }
}
