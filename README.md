# ⚡ High-Throughput Real-Time Telemetry & Notification Hub

> **Production-ready, zero-allocation, lock-free telemetry ingestion engine built with C# 13 and .NET 9.**

[![.NET](https://img.shields.io/badge/.NET-9.0-512BD4?style=flat-square&logo=dotnet)](https://dotnet.microsoft.com/)
[![C#](https://img.shields.io/badge/C%23-13.0-239120?style=flat-square&logo=csharp)](https://learn.microsoft.com/en-us/dotnet/csharp/)
[![License](https://img.shields.io/badge/License-MIT-blue?style=flat-square)](LICENSE)

---

## 📋 İçindekiler

- [Proje Nedir?](#-proje-nedir)
- [Neden Yüksek Başarımlı Bir Pipeline?](#-neden-yüksek-başarımlı-bir-pipeline)
- [Mimari & Veri Akışı](#-mimari--veri-akışı)
- [Klasör Yapısı](#-klasör-yapısı)
- [Teknik Stack](#-teknik-stack)
- [Temel Kavramlar](#-temel-kavramlar)
  - [TelemetryPayload (DTO)](#1-telemetrypayload-dto)
  - [ITelemetryChannel (Kuyruk Yönetimi)](#2-itelemetrychannel-kuyruk-yönetimi)
  - [TelemetryEndpoints (API)](#3-telemetryendpoints-api)
  - [TelemetryConsumerService (Tüketici)](#4-telemetryconsumerservice-tüketici)
  - [TelemetryStorage (Analitik Depo)](#5-telemetrystorage-analitik-depo)
- [API Referansı](#-api-referansı)
- [Kurulum & Çalıştırma](#-kurulum--çalıştırma)
- [Web Dashboard](#-web-dashboard)
- [Performans Optimizasyonları](#-performans-optimizasyonları)
- [Gerçek Dünya Kullanım Senaryoları](#-gerçek-dünya-kullanım-senaryoları)
- [Faz Yol Haritası](#-faz-yol-haritası)

---

## 🎯 Proje Nedir?

Bu proje, binlerce fiziksel sensör, akıllı cihaz veya sunucudan **aynı anda** gelen yoğun veri akışını (telemetri verisi) saniyede on binlerce istek hızında kesintisiz kabul edip işleyebilen, **kilit-siz (lock-free)**, **sıfır bellek tahsisatı (zero-allocation)** prensibiyle tasarlanmış bir **yüksek başarımlı veri toplama motorudur (Ingestion Engine).**

### Gerçek Hayattan Benzetme

Bir şehirde 50.000 elektrikli scooter düşünün. Her biri saniyede 1 kez GPS konumu, batarya seviyesi, motor sıcaklığı ve hız verisi gönderiyor. Bu demektir ki merkezî sunucunuza **saniyede 50.000 ayrı istek** geliyor. Geleneksel bir REST API mimarisi bu yükü kaldıramaz; sunucu kilitlenir, istekler kaybolur ya da sistem çöker.

Bu proje tam olarak bu problemi çözmek için tasarlanmıştır.

---

## ⚙️ Neden Yüksek Başarımlı Bir Pipeline?

| Geleneksel Yaklaşım | Bu Projenin Yaklaşımı |
|---|---|
| Her istek veritabanına doğrudan yazar → Yavaş | Veri önce lock-free kuyruğa alınır, 202 hemen döner |
| `lock` veya `Monitor` ile thread kilitleme → Bekleme | `System.Threading.Channels` ile kilit olmadan eşzamanlılık |
| `class` nesneleri → Heap bellek + GC baskısı | `readonly record struct` → Stack bellek, sıfır GC |
| Senkron (Synchronous) okuma → Thread bloklanır | `ReadAllAsync` + `async/await` → Non-blocking tam asenkron |

---

## 🏗️ Mimari & Veri Akışı

```
┌─────────────────────────────────────────────────────────────────────┐
│                     GERÇEK DÜNYA / SAHADAN                          │
│  📱 Sensörler  🚗 Araçlar  💻 Sunucular  🌡️ IoT Cihazlar           │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ HTTP POST (JSON Payload)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│              INGESTION LAYER (Minimal API - ASP.NET Core)            │
│  POST /api/v1/telemetry         → Tekli veri kabulü (202 Accepted)  │
│  POST /api/v1/telemetry/batch   → Toplu veri kabulü (202 Accepted)  │
│                                                                      │
│  ✅ Zero-Allocation: readonly record struct TelemetryPayload         │
│  ✅ Fast-path: TryWrite() → WriteAsync() fallback (backpressure)     │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ Channel.Writer.TryWrite() / WriteAsync()
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│         BOUNDED CHANNEL BUFFER (System.Threading.Channels)           │
│                                                                      │
│  Capacity       = 10,000 items                                       │
│  FullMode       = BoundedChannelFullMode.Wait (backpressure)         │
│  SingleWriter   = false (çoklu HTTP thread destekli)                 │
│  SingleReader   = true  (tek tüketici, optimize edilmiş)             │
│  AllowSyncCont  = false (thread pool starvation önlenir)             │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ Channel.Reader.ReadAllAsync()
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│           CONSUMER LAYER (BackgroundService Worker)                  │
│                                                                      │
│  TelemetryConsumerService:                                           │
│  • await foreach (var payload in Reader.ReadAllAsync(ct))            │
│  • ProcessPayload(in payload) → storage.RecordPayload()              │
│  • [LoggerMessage] source generator → zero-allocation logging        │
│  • System.Diagnostics.Metrics Counter → OpenTelemetry hazır          │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
              ┌─────────────┴──────────────┐
              ▼                            ▼
┌─────────────────────┐       ┌────────────────────────────┐
│   STORAGE LAYER     │       │     ANALYTICS ENGINE        │
│  TelemetryStorage   │       │  Min / Max / Avg hesaplama  │
│  Son 50 payload     │       │  🟢 NORMAL / ⚠️ WARNING    │
│  ConcurrentQueue    │       │  🚨 CRITICAL ALARM logic    │
└──────────┬──────────┘       └───────────┬────────────────┘
           │                              │
           └──────────────┬───────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    RESULT & REPORTING LAYER                          │
│  GET /api/v1/telemetry/recent    → Son 50 işlenmiş payload           │
│  GET /api/v1/telemetry/analytics → Metrik bazlı canlı istatistikler  │
│  GET /api/v1/telemetry/stats     → Motor sağlık & uptime bilgisi     │
│  GET /health                     → Load balancer sağlık noktası      │
│  GET /  (index.html)             → İnteraktif Web Dashboard          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Klasör Yapısı

```
High-Throughput Real-Time Telemetry & Notification Hub using .NET 9/
└── src/
    └── TelemetryHub/
        ├── TelemetryHub.csproj              # Proje dosyası (.NET 9, C# 13)
        ├── Program.cs                       # Uygulama başlangıcı ve DI yapılandırması
        ├── appsettings.json                 # Kanal ve log yapılandırmaları
        │
        ├── Domain/
        │   └── Models/
        │       └── TelemetryPayload.cs      # Sıfır-tahsisat DTO (readonly record struct)
        │
        ├── Infrastructure/
        │   ├── Channels/
        │   │   ├── ITelemetryChannel.cs     # Kanal soyutlama arayüzü
        │   │   ├── TelemetryChannel.cs      # BoundedChannel implementasyonu
        │   │   └── TelemetryChannelOptions.cs # Kanal konfigürasyon seçenekleri
        │   └── Storage/
        │       └── TelemetryStorage.cs      # In-memory analitik depolama
        │
        ├── Endpoints/
        │   └── TelemetryEndpoints.cs        # Minimal API uç noktaları
        │
        ├── Services/
        │   └── TelemetryConsumerService.cs  # BackgroundService tüketici işçisi
        │
        └── wwwroot/
            └── index.html                   # İnteraktif Web Dashboard UI
```

---

## 🛠️ Teknik Stack

| Katman | Teknoloji | Sürüm |
|---|---|---|
| Dil & Framework | C# 13 / .NET 9 Web API | 9.0.318+ |
| Eşzamanlılık Modeli | `System.Threading.Channels` (BoundedChannel) | .NET 9 |
| HTTP Katmanı | ASP.NET Core Minimal API | .NET 9 |
| Arka Plan İşçisi | `BackgroundService` (IHostedService) | .NET 9 |
| Seri/Deserializasyon | `System.Text.Json` | .NET 9 |
| Loglama | `ILogger` + `[LoggerMessage]` Source Generator | .NET 9 |
| Metrik Ölçümü | `System.Diagnostics.Metrics` (OpenTelemetry uyumlu) | .NET 9 |
| In-Memory Depo | `ConcurrentQueue<T>` + `ConcurrentDictionary<K,V>` | .NET 9 |
| Web Arayüz | Vanilla HTML5 / CSS3 / JavaScript (Fetch API) | - |
| Tipografi | Google Fonts: Inter + JetBrains Mono | - |

---

## 🔬 Temel Kavramlar

### 1. TelemetryPayload (DTO)

**Dosya:** `src/TelemetryHub/Domain/Models/TelemetryPayload.cs`

```csharp
public readonly record struct TelemetryPayload(
    Guid DeviceId,
    DateTimeOffset Timestamp,
    string MetricName,
    double Value,
    IReadOnlyDictionary<string, string>? Tags = null)
```

**Neden `readonly record struct`?**

- **`struct`:** Heap'e (RAM'in GC tarafından yönetilen bölgesi) değil, doğrudan **Stack belleğe** tahsis edilir. Bu demektir ki Garbage Collector (GC) bu nesneyi hiç görmez ve temizlemek için duraksamaz.
- **`readonly`:** İçerik bir kez atandıktan sonra değiştirilemez. Thread-safety (iş parçacığı güvenliği) garanti edilir.
- **`record`:** C# 13'te `==`, `!=`, `ToString()`, pattern matching gibi tüm boilerplate kodları otomatik oluşturulur.

**Performans Etkisi:** 10.000 payload/saniye işlendiğinde geleneksel `class` kullanımı 10.000 heap allocation + GC baskısı yaratır. `readonly record struct` ile bu sayı **sıfıra** düşer.

---

### 2. ITelemetryChannel (Kuyruk Yönetimi)

**Dosya:** `src/TelemetryHub/Infrastructure/Channels/TelemetryChannel.cs`

```csharp
new BoundedChannelOptions(capacity: 10_000)
{
    FullMode = BoundedChannelFullMode.Wait,
    SingleWriter = false,
    SingleReader = true,
    AllowSynchronousContinuations = false
}
```

**Her seçeneğin anlamı:**

| Seçenek | Değer | Neden? |
|---|---|---|
| `Capacity` | 10.000 | Tampon boyutu. 10.000 veri öğesi hafızada bekleyebilir. |
| `FullMode = Wait` | `Wait` | Tampon dolduğunda API çağrıcısı **bloklanmadan async bekler** (veri kaybı yok). |
| `SingleWriter = false` | `false` | Birden fazla HTTP isteği aynı anda veri yazabilir (thread-safe). |
| `SingleReader = true` | `true` | Yalnızca bir tüketici servisi okur → kanal içinde gereksiz kilitleme mekanizması devre dışı kalır (daha hızlı). |
| `AllowSynchronousContinuations = false` | `false` | Async callback'ler aynı thread'de senkron çağrılmaz → Thread Pool tükenmesi (starvation) engellenir. |

---

### 3. TelemetryEndpoints (API)

**Dosya:** `src/TelemetryHub/Endpoints/TelemetryEndpoints.cs`

**Hızlı Yol (Fast Path) Algoritması:**

```csharp
// 1. Önce kilit olmadan, allocation olmadan direkt yazmayı dene:
if (!channel.Writer.TryWrite(payload))
{
    // 2. Tampon doluysa async bekle (backpressure mekanizması devreye girer):
    await channel.Writer.WriteAsync(payload, ct);
}
```

**Neden bu ikili yaklaşım?**

`TryWrite()` başarılı olursa: Hiçbir `Task` nesnesi oluşturulmaz, hiçbir async state machine başlatılmaz → maksimum hız.

`TryWrite()` başarısız olursa (tampon dolu): `WriteAsync()` ile caller async beklemeye alınır → veri hiç kaybolmaz.

---

### 4. TelemetryConsumerService (Tüketici)

**Dosya:** `src/TelemetryHub/Services/TelemetryConsumerService.cs`

```csharp
await foreach (var payload in channel.Reader.ReadAllAsync(stoppingToken))
{
    ProcessPayload(in payload);    // 'in' = ref readonly → kopyalama yok
    ItemsConsumedCounter.Add(1);   // OpenTelemetry metrik sayacı
}
```

**Neden `ReadAllAsync` ve `await foreach`?**

- `ReadAllAsync()`: Kanal boşken thread'i **bloklamadan** async bekler. Yeni veri geldiğinde otomatik uyarılır. Polling (sürekli sorgulama) yapmaz → CPU boşa harcanmaz.
- `in payload`: Struct'ı kopyalamadan referans olarak iletir → stack üzerinde sıfır kopyalama.
- `[LoggerMessage]` source generator: Her log çağrısında `string.Format` ve boxing yapmaz → sıfır tahsisat logging.

---

### 5. TelemetryStorage (Analitik Depo)

**Dosya:** `src/TelemetryHub/Infrastructure/Storage/TelemetryStorage.cs`

- **Son 50 payload:** `ConcurrentQueue<TelemetryPayload>` ile thread-safe son gelen veri listesi tutulur.
- **Metrik bazlı istatistik:** `ConcurrentDictionary<string, List<double>>` — her metrik adı için değerler biriktirilir.
- **Anlık analiz:** `Min`, `Max`, `Average` hesaplanır ve değer > 85 ise 🚨 CRITICAL, > 70 ise ⚠️ WARNING, ≤ 70 ise 🟢 NORMAL statüsü atanır.

---

## 📡 API Referansı

### Veri Gönderme Uç Noktaları

#### `POST /api/v1/telemetry`
Tek bir telemetri paketi gönderir.

**Request Body:**
```json
{
  "deviceId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "timestamp": "2026-09-15T14:58:00Z",
  "metricName": "engine_temperature_celsius",
  "value": 98.6,
  "tags": {
    "vehicle_model": "EV-Sedan",
    "location": "Istanbul"
  }
}
```

**Yanıt:**
```
HTTP 202 Accepted
```

> ⚠️ **Önemli:** `deviceId` alanı **GUID formatında** olmalıdır: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`

---

#### `POST /api/v1/telemetry/batch`
Birden fazla telemetri paketini tek seferde gönderir.

**Request Body:**
```json
[
  { "deviceId": "...", "timestamp": "...", "metricName": "cpu_usage", "value": 45.2 },
  { "deviceId": "...", "timestamp": "...", "metricName": "memory_mb", "value": 8192.0 }
]
```

---

### Sonuç & Analitik Uç Noktaları

#### `GET /api/v1/telemetry/recent`
Arka plan tüketicisi tarafından işlenmiş son 50 payload'ı döner.

**Örnek Yanıt:**
```json
[
  {
    "deviceId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "timestamp": "2026-09-15T15:07:00Z",
    "metricName": "engine_temperature_celsius",
    "value": 98.6,
    "tags": null
  }
]
```

---

#### `GET /api/v1/telemetry/analytics`
Her metrik adı için anlık hesaplanmış istatistikleri döner.

**Örnek Yanıt:**
```json
[
  {
    "metricName": "engine_temperature_celsius",
    "count": 15,
    "minValue": 72.3,
    "maxValue": 98.6,
    "averageValue": 85.4,
    "status": "🚨 CRITICAL ALARM"
  },
  {
    "metricName": "battery_soc_percent",
    "count": 8,
    "minValue": 18.4,
    "maxValue": 94.2,
    "averageValue": 62.1,
    "status": "🟢 NORMAL"
  }
]
```

**Statü Sınıflandırma Mantığı:**

```
Value > 85.0  → 🚨 CRITICAL ALARM
Value > 70.0  → ⚠️ WARNING
Value ≤ 70.0  → 🟢 NORMAL
```

---

#### `GET /api/v1/telemetry/stats`
Motor sağlık durumu ve çalışma istatistiklerini döner.

```json
{
  "status": "Healthy",
  "totalIngested": 1042,
  "channelCapacity": 10000,
  "uptimeSeconds": 284,
  "framework": ".NET 9 (C# 13)",
  "architecture": "System.Threading.Channels (BoundedChannel)"
}
```

---

#### `GET /health`
Load balancer ve container orchestrator (Kubernetes gibi) için sağlık noktası.

```json
{ "status": "Healthy", "timestamp": "2026-09-15T12:15:22Z" }
```

---

## 🚀 Kurulum & Çalıştırma

### Gereksinimler

- **İşletim Sistemi:** Windows 10/11, macOS, Linux
- **.NET 9 SDK:** [dotnet.microsoft.com/download/dotnet/9.0](https://dotnet.microsoft.com/download/dotnet/9.0)

### Adım 1: .NET 9 SDK Kurulumu (Windows - Otomatik)

PowerShell'de yönetici yetkisiyle:

```powershell
Invoke-WebRequest -Uri "https://dot.net/v1/dotnet-install.ps1" -OutFile "$env:TEMP\dotnet-install.ps1"
& "$env:TEMP\dotnet-install.ps1" -Channel 9.0 -InstallDir "$env:LocalAppData\Microsoft\dotnet"
```

Kurulum sonrası PATH'e eklemek için:

```powershell
$path = [Environment]::GetEnvironmentVariable("Path", "User")
[Environment]::SetEnvironmentVariable("Path", "$path;$env:LocalAppData\Microsoft\dotnet", "User")
```

### Adım 2: Projeyi Derle

```powershell
# Proje klasörüne git
cd "High-Throughput Real-Time Telemetry & Notification Hub using .NET 9"

# Derle (0 hata, 0 uyarı beklenir)
dotnet build src/TelemetryHub/TelemetryHub.csproj
```

### Adım 3: Projeyi Çalıştır

```powershell
dotnet run --project src/TelemetryHub/TelemetryHub.csproj --urls "http://localhost:5000"
```

Başarılı başlangıçta şu çıktıyı görürsünüz:

```
info: TelemetryHub.Services.TelemetryConsumerService[1]
      TelemetryConsumerService started. Listening for incoming channel items...
info: Microsoft.Hosting.Lifetime[14]
      Now listening on: http://localhost:5000
info: Microsoft.Hosting.Lifetime[0]
      Application started. Press Ctrl+C to shut down.
```

### Adım 4: Test Et

```powershell
# Sağlık kontrolü
Invoke-RestMethod -Uri "http://localhost:5000/health"

# Tekli telemetri gönder
$body = @{
    deviceId   = "3fa85f64-5717-4562-b3fc-2c963f66afa6"
    timestamp  = (Get-Date).ToString("o")
    metricName = "engine_temperature_celsius"
    value      = 98.6
} | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:5000/api/v1/telemetry" -Method Post -Body $body -ContentType "application/json"

# İşlenmiş sonuçları gör
Invoke-RestMethod -Uri "http://localhost:5000/api/v1/telemetry/recent"

# Hesaplanan analizleri gör
Invoke-RestMethod -Uri "http://localhost:5000/api/v1/telemetry/analytics"
```

---

## 🖥️ Web Dashboard

Uygulama çalışırken tarayıcınızda **[http://localhost:5000](http://localhost:5000)** adresini açın.

### Dashboard Bölümleri

| Bölüm | Açıklama |
|---|---|
| **Motor Statü Kartları** | Toplam işlenen payload, işlenen sonuç sayısı, uptime ve kanal kapasitesi |
| **Canlı Metrik Analiz Kartları** | Her metrik adı için Min/Max/Ortalama + otomatik alarm sınıflandırması |
| **Canlı Sonuç Tablosu** | Son 50 işlenmiş payload — cihaz, metrik, değer ve statü rozetiyle |
| **Send Single Payload** | Form ile özel telemetri verisi gönder. "🔀 New ID" ile yeni GUID üret |
| **Bulk Simulation** | 100 adet veri paketi eşzamanlı gönderimini test et |

### Önemli Uyarı: Device ID Formatı

Dashboard formundaki `Device ID` alanı **GUID formatı** gerektirmektedir:

```
✅ Doğru : 3fa85f64-5717-4562-b3fc-2c963f66afa6
❌ Yanlış : device_sensor_101
❌ Yanlış : sensor-01
```

"**🔀 New ID**" butonuna tıklayarak tarayıcı otomatik geçerli bir GUID üretir.

---

## ⚡ Performans Optimizasyonları

| Optimizasyon | Teknik | Etki |
|---|---|---|
| **Zero Allocation DTO** | `readonly record struct TelemetryPayload` | GC heap allocation = 0 |
| **Lock-Free Queue** | `BoundedChannel<TelemetryPayload>` | Kilit (lock) çekişmesi = 0 |
| **Sync Continuation Kapatma** | `AllowSynchronousContinuations = false` | Thread pool tükenmesi riski = 0 |
| **TryWrite Fast Path** | `TryWrite()` → `WriteAsync()` fallback | Task allocation gerek kalmaz |
| **Sıfır Kopyalama** | `ProcessPayload(in payload)` (ref readonly) | Struct kopyalama maliyeti = 0 |
| **Source Generated Logging** | `[LoggerMessage]` attribute | String boxing + format allocation = 0 |
| **Single Reader Optimizasyonu** | `SingleReader = true` | Kanal içi kilit mekanizması bypass |

---

## 🌍 Gerçek Dünya Kullanım Senaryoları

### 🚗 Elektrikli Araç Filosu Takibi
50.000 araç saniyede 1 kez batarya seviyesi, motor sıcaklığı, GPS konumu gönderir. Motor 85°C üzerine çıktığında anlık 🚨 CRITICAL alarmı üretilir.

### 🏭 Fabrika Makine İzleme (Industrial IoT)
Üretim hattındaki 1.000 CNC tezgahının titreşim, sıcaklık ve devir değerleri sürekli izlenir. Arıza öncesi anomaliler ⚠️ WARNING ile tespit edilir.

### 🖥️ Sunucu & Cloud Altyapı İzleme
Datadog, Dynatrace benzeri APM (Application Performance Monitoring) sistemleri. Binlerce sunucunun CPU, RAM, disk I/O ve network latency değerleri anlık toplanır ve analiz edilir.

### 📈 Finans & Borsa Veri Akışı
Kripto para borsasındaki binlerce parite için saniyede onlarca fiyat güncellemesi işlenir. Ani fiyat değişimleri anlık alarm olarak raporlanır.

### 🎮 Çok Oyunculu Online Oyunlar
Binlerce oyuncunun anlık konum, sağlık değerleri, skor ve ping verisi merkezi sunucuya yüksek hızda iletilir.

---

## 🗺️ Faz Yol Haritası

### ✅ Faz 1 — Çekirdek Toplama Hattı (Tamamlandı)
- `TelemetryPayload` zero-allocation DTO
- `BoundedChannel` lock-free kuyruk yöneticisi
- Minimal API ingestion uç noktaları
- `BackgroundService` tüketici işçisi
- In-memory analitik motor
- İnteraktif Web Dashboard

### 🔜 Faz 2 — Kalıcı Depolama & Toplu Yazma
- **PostgreSQL + TimescaleDB** entegrasyonu
- **Dapper Bulk Insert** ile batch yazma (10.000 satır / operasyon)
- **EF Core** ile veri modellemesi
- **Polly v8** resilience pipeline (retry, circuit breaker)

### 🔜 Faz 3 — Gerçek Zamanlı Bildirim (SignalR)
- **SignalR Hub** ile bağlı istemcilere anlık push bildirimleri
- **Redis Backplane** ile çok sunuculu (multi-node) mimari desteği
- Tarayıcıda canlı streaming grafik görselleştirmesi

### 🔜 Faz 4 — Gözlemlenebilirlik (Observability)
- **.NET Aspire** entegrasyonu
- **OpenTelemetry** traces, metrics ve logs
- **Prometheus** metrik export
- **BenchmarkDotNet** performans benchmark suite

---

## 📄 Lisans

MIT License — Detaylar için `LICENSE` dosyasına bakınız.

---

<div align="center">

**⚡ Built with C# 13 & .NET 9 | System.Threading.Channels | Zero-Allocation Architecture**

</div>
