const state = {
  brokers: [],
  brokerProfiles: [],
  presets: [],
  rateProfiles: {},
  runs: [],
  events: [],
  selectedBrokerId: "kafka",
  protocolByBroker: {},
  serverTime: null,
  wizardStep: 1,
  configScope: "broker",
  tuningPresetKind: "latency",
  tuningModel: { broker: {}, producer: {}, consumer: {} },
  vizSelectedIds: new Set(),
  reportSelectedIds: new Set(),
  vizSelectionTouched: false,
  reportSelectionTouched: false,
  activeTab: "benchmark",
  staticLoaded: false,
  bootstrapStatus: null,
  reportSections: new Set(["summary", "comparison", "details", "configuration", "artifacts"]),
  resourceConfigByBroker: {},
  vizAnimationHandle: 0,
  vizAnimationSignature: "",
};

const palette = ["#43b4ff", "#4dffc5", "#ffbd59", "#ff6d9e", "#a78bfa", "#36f8ff"];
const REPORT_SECTION_OPTIONS = [
  { id: "summary", label: "Summary table" },
  { id: "comparison", label: "Comparison charts" },
  { id: "details", label: "Per-run charts" },
  { id: "configuration", label: "Configuration appendix" },
  { id: "artifacts", label: "Artifact appendix" },
];
const RESOURCE_CONFIG_KEYS = ["cpuRequest", "cpuLimit", "memoryRequest", "memoryLimit", "storageSize", "storageClassName"];
const SETUP_PRESETS = {
  latency: { label: "Performance", configMode: "optimized", tuningPreset: "latency" },
  throughput: { label: "Throughput", configMode: "optimized", tuningPreset: "throughput" },
  balanced: { label: "Balanced", configMode: "optimized", tuningPreset: "balanced" },
  durable: { label: "Durable", configMode: "baseline", tuningPreset: "durable" },
  defaults: { label: "Standard", configMode: "baseline", tuningPreset: "profile" },
};

const CONFIG_FIELDS = {
  kafka: {
    broker: [
      { key: "defaultPartitions", label: "Default partitions", help: "Partition count created for benchmark topics. More partitions can improve parallelism, but they also add routing work.", type: "number", min: 1, step: 1, defaultValue: 3, tunedValue: 6, balancedValue: 3, throughputValue: 12 },
      { key: "defaultReplicationFactor", label: "Default replication factor", help: "Replica count used for benchmark topics. More replicas usually increase durable write latency.", type: "number", min: 1, step: 1, defaultValue: 1, tunedValue: 1, balancedValue: 2, durableValue: 3 },
      { key: "minInSyncReplicas", label: "Min in-sync replicas", help: "Minimum replicas that must confirm a write. Higher values increase durability and can add publish latency.", type: "number", min: 1, step: 1, defaultValue: 1, tunedValue: 1, balancedValue: 1, durableValue: 2 },
      { key: "messageMaxBytes", label: "Max message bytes", help: "Largest record the broker accepts. Keep it above your benchmark payload size to avoid rejects.", type: "number", min: 1024, step: 1024, defaultValue: 1048576, tunedValue: 2097152 },
      { key: "queuedMaxRequests", label: "Queued max requests", help: "Maximum client requests waiting inside the broker. Too low throttles early; too high can hide saturation.", type: "number", min: 1, step: 1, defaultValue: 500, tunedValue: 1000, throughputValue: 2000 },
      { key: "replicaFetchMaxBytes", label: "Replica fetch max bytes", help: "Maximum bytes followers pull per replication fetch. Larger values help larger messages and replicas catch up faster.", type: "number", min: 1024, step: 1024, defaultValue: 1048576, tunedValue: 10485760, throughputValue: 10485760 },
      { key: "numNetworkThreads", label: "Network threads", help: "Broker threads that accept requests and send responses. Too few can bottleneck first-byte latency.", type: "number", min: 1, step: 1, defaultValue: 3, tunedValue: 8, throughputValue: 12 },
      { key: "numIoThreads", label: "I/O threads", help: "Broker threads used for request and disk work. More threads can help steady-state latency under load.", type: "number", min: 1, step: 1, defaultValue: 8, tunedValue: 16, throughputValue: 20 },
      { key: "numReplicaFetchers", label: "Replica fetchers", help: "Follower replication threads. This mainly affects replicated write latency and lag recovery.", type: "number", min: 1, step: 1, defaultValue: 1, tunedValue: 2, throughputValue: 4 },
      { key: "socketSendBufferBytes", label: "Socket send buffer", help: "Broker TCP send buffer size. Larger buffers can smooth bursts, but too large can hide pressure.", type: "number", min: 1024, step: 1024, defaultValue: 102400, tunedValue: 1048576, throughputValue: 1048576 },
      { key: "socketReceiveBufferBytes", label: "Socket receive buffer", help: "Broker TCP receive buffer size. This affects how quickly incoming traffic is absorbed.", type: "number", min: 1024, step: 1024, defaultValue: 102400, tunedValue: 1048576, throughputValue: 1048576 },
      { key: "logFlushIntervalMessages", label: "Log flush interval messages", help: "Number of messages before a forced flush. Lower values add fsync overhead but tighten durability.", type: "number", min: 1, step: 1, defaultValue: 10000, tunedValue: 50000, throughputValue: 50000 },
      { key: "logFlushIntervalMs", label: "Log flush interval ms", help: "Maximum time before a log flush. Lower values reduce commit latency but increase I/O load.", type: "number", min: 1, step: 1, defaultValue: 1000, tunedValue: 5000, throughputValue: 5000 },
      { key: "logSegmentBytes", label: "Log segment bytes", help: "Size of log segments. Smaller segments increase log rotation frequency.", type: "number", min: 1048576, step: 1048576, defaultValue: 1073741824, tunedValue: 268435456 },
    ],
    producer: [
      { key: "acks", label: "Acks", help: "Write acknowledgment mode. Lower values reduce producer wait time, but they also reduce durability guarantees.", type: "select", options: [{ value: "0", label: "0" }, { value: "1", label: "1" }, { value: "all", label: "all" }], defaultValue: "1", tunedValue: "1", balancedValue: "1", durableValue: "all" },
      { key: "lingerMs", label: "Linger ms", help: "Time the producer waits to batch records. Lower values reduce queueing delay, higher values improve batching efficiency.", type: "number", min: 0, step: 1, defaultValue: 0, tunedValue: 0, throughputValue: 12, balancedValue: 1, durableValue: 4 },
      { key: "batchSizeBytes", label: "Batch size bytes", help: "Maximum batch size per partition before send. Larger batches improve throughput but can increase first-message latency.", type: "number", min: 1024, step: 1024, defaultValue: 16384, tunedValue: 16384, throughputValue: 262144, balancedValue: 32768, durableValue: 32768 },
      { key: "compressionType", label: "Compression", help: "Producer compression mode. Compression saves bandwidth, but it adds CPU and can increase latency for small messages.", type: "select", options: [{ value: "none", label: "none" }, { value: "snappy", label: "snappy" }, { value: "lz4", label: "lz4" }, { value: "zstd", label: "zstd" }], defaultValue: "none", tunedValue: "none", throughputValue: "lz4" },
      { key: "socketNagleDisable", label: "Disable Nagle", help: "Disable Nagle on producer sockets so small records are pushed immediately instead of waiting for packet coalescing.", type: "boolean", defaultValue: false, tunedValue: true, balancedValue: true },
      { key: "maxInFlightRequests", label: "Max in-flight requests", help: "Maximum unacknowledged requests per connection. Lower values reduce reordering risk during retries.", type: "number", min: 1, step: 1, defaultValue: 5, tunedValue: 5, throughputValue: 5, balancedValue: 2, durableValue: 1 },
      { key: "requestTimeoutMs", label: "Request timeout ms", help: "How long the producer waits for a broker response before retrying or failing.", type: "number", min: 1000, step: 1000, defaultValue: 30000, tunedValue: 10000, balancedValue: 15000 },
      { key: "bufferMemoryBytes", label: "Buffer memory bytes", help: "Producer-side memory used to queue unsent records. Too little can cause local backpressure sooner.", type: "number", min: 1048576, step: 1048576, defaultValue: 33554432, tunedValue: 67108864, throughputValue: 134217728 },
      { key: "maxRequestSizeBytes", label: "Max request bytes", help: "Largest produce request the client sends. Keep it above your message and batch size.", type: "number", min: 1024, step: 1024, defaultValue: 1048576, tunedValue: 2097152, throughputValue: 10485760 },
      { key: "deliveryTimeoutMs", label: "Delivery timeout ms", help: "Upper bound on time to report success or failure after send(). Must be >= linger.ms + request.timeout.ms.", type: "number", min: 1000, step: 1000, defaultValue: 120000, tunedValue: 20000, balancedValue: 30000 },
    ],
    consumer: [
      { key: "fetchMinBytes", label: "Fetch min bytes", help: "Minimum bytes the broker waits for before replying to a fetch. Lower values reduce receive latency.", type: "number", min: 1, step: 1, defaultValue: 1, tunedValue: 1, throughputValue: 65536 },
      { key: "fetchMaxWaitMs", label: "Fetch max wait ms", help: "Maximum time the broker waits before replying to a fetch. Lower values reduce fetch delay.", type: "number", min: 0, step: 1, defaultValue: 500, tunedValue: 1, throughputValue: 50, balancedValue: 50, durableValue: 100 },
      { key: "maxPollRecords", label: "Max poll records", help: "Maximum records returned per poll. Larger values improve throughput but can increase per-poll processing time.", type: "number", min: 1, step: 1, defaultValue: 500, tunedValue: 32, throughputValue: 2000, balancedValue: 256, durableValue: 500 },
      { key: "maxPartitionFetchBytes", label: "Max partition fetch bytes", help: "Maximum bytes fetched per partition in one request. Increase it for larger messages.", type: "number", min: 1024, step: 1024, defaultValue: 1048576, tunedValue: 1048576, throughputValue: 4194304 },
      { key: "fetchMaxBytes", label: "Fetch max bytes", help: "Maximum total bytes returned by one fetch request.", type: "number", min: 1024, step: 1024, defaultValue: 52428800, tunedValue: 52428800, throughputValue: 67108864 },
      { key: "sessionTimeoutMs", label: "Session timeout ms", help: "Consumer group session timeout. It affects group stability more than steady-state latency.", type: "number", min: 1000, step: 1000, defaultValue: 45000, tunedValue: 30000 },
      { key: "heartbeatIntervalMs", label: "Heartbeat interval ms", help: "Consumer group heartbeat cadence. Lower values detect failures sooner but add more coordinator traffic.", type: "number", min: 500, step: 500, defaultValue: 3000, tunedValue: 1000, balancedValue: 1500 },
      { key: "enableAutoCommit", label: "Auto commit", help: "Automatic offset commit. Disabling it removes commit noise from the hot path.", type: "boolean", defaultValue: false, tunedValue: false },
      { key: "maxPollIntervalMs", label: "Max poll interval ms", help: "Maximum delay between poll() calls before the consumer is considered failed.", type: "number", min: 1000, step: 1000, defaultValue: 300000, tunedValue: 60000 },
      { key: "socketNagleDisable", label: "Disable Nagle", help: "Disable Nagle on consumer sockets so fetch responses are not delayed by packet coalescing.", type: "boolean", defaultValue: false, tunedValue: true, balancedValue: true },
    ],
  },
  rabbitmq: {
    broker: [
      { key: "defaultQueueType", label: "Default queue type", help: "Queue type created for the benchmark. Quorum is safer but usually slower than classic for pure latency tests.", type: "select", options: [{ value: "classic", label: "classic" }, { value: "quorum", label: "quorum" }], defaultValue: "classic", tunedValue: "quorum", balancedValue: "classic", durableValue: "quorum" },
      { key: "vmMemoryHighWatermark", label: "VM memory high watermark", help: "Memory pressure threshold. When the broker crosses it, flow control starts and publish latency rises sharply.", type: "number", min: 0.1, max: 1, step: 0.05, defaultValue: 0.4, tunedValue: 0.4 },
      { key: "diskFreeLimitRelative", label: "Disk free limit", help: "Disk alarm threshold before RabbitMQ slows or blocks publishers. Lower values delay the alarm but reduce safety margin.", type: "number", min: 0.1, max: 2, step: 0.05, defaultValue: 1, tunedValue: 0.8 },
      { key: "consumerTimeoutMs", label: "Consumer timeout ms", help: "Server-side timeout for blocked consumers. This matters more for long test stability than for hot-path latency.", type: "number", min: 1000, step: 1000, defaultValue: 1800000, tunedValue: 600000 },
      { key: "collectStatisticsIntervalMs", label: "Statistics interval ms", help: "Broker metrics sampling interval. Faster sampling gives more detail but adds some broker work.", type: "number", min: 1000, step: 1000, defaultValue: 5000, tunedValue: 5000, throughputValue: 10000 },
      { key: "clusterPartitionHandling", label: "Cluster partition handling", help: "How the cluster reacts to partitions. This affects availability behavior more than steady-state latency.", type: "select", options: [{ value: "ignore", label: "ignore" }, { value: "pause_minority", label: "pause_minority" }, { value: "autoheal", label: "autoheal" }], defaultValue: "pause_minority", tunedValue: "autoheal" },
      { key: "handshakeTimeoutMs", label: "Handshake timeout ms", help: "Maximum time allowed for AMQP handshake. It affects connection setup, not message-path latency.", type: "number", min: 1000, step: 1000, defaultValue: 10000, tunedValue: 5000 },
      { key: "tcpBacklog", label: "TCP backlog", help: "Pending TCP accept queue size. This helps absorb bursts of client connections during startup.", type: "number", min: 32, step: 32, defaultValue: 128, tunedValue: 256, throughputValue: 512 },
    ],
    producer: [
      { key: "publisherConfirms", label: "Publisher confirms", help: "Wait for broker publish confirms. This adds confirmation latency, but it measures durable delivery more honestly.", type: "boolean", defaultValue: true, tunedValue: false, durableValue: true },
      { key: "mandatory", label: "Mandatory publish", help: "Fail publish when routing does not resolve. Useful for correctness, but not a steady-state latency knob.", type: "boolean", defaultValue: false, tunedValue: false },
      { key: "heartbeatSec", label: "Heartbeat sec", help: "AMQP heartbeat interval. This mostly affects connection health detection.", type: "number", min: 1, step: 1, defaultValue: 30, tunedValue: 30 },
      { key: "frameMax", label: "Frame max bytes", help: "Maximum AMQP frame size. AMQP 0-9-1 caps this at 131072 bytes, so higher values are invalid and will be rejected.", type: "number", min: 4096, max: 131072, step: 4096, defaultValue: 131072, tunedValue: 131072, throughputValue: 131072 },
      { key: "channelMax", label: "Channel max", help: "Maximum channels allowed per connection. Higher values help dense clients, but they do not directly reduce latency.", type: "number", min: 1, step: 1, defaultValue: 2047, tunedValue: 2047 },
      { key: "connectionTimeoutMs", label: "Connection timeout ms", help: "Maximum time allowed to establish the producer connection.", type: "number", min: 1000, step: 1000, defaultValue: 30000, tunedValue: 15000 },
    ],
    consumer: [
      { key: "prefetch", label: "Prefetch", help: "Messages allowed in flight to the consumer before ack. Lower values reduce local queueing; higher values improve throughput.", type: "number", min: 0, step: 1, defaultValue: 0, tunedValue: 32, throughputValue: 1000, balancedValue: 128, durableValue: 50 },
      { key: "prefetchGlobal", label: "Prefetch global", help: "Apply the prefetch limit at channel scope instead of per consumer.", type: "boolean", defaultValue: false, tunedValue: false },
      { key: "autoAck", label: "Auto ack", help: "Acknowledge on delivery instead of after processing. This lowers ack overhead but weakens delivery guarantees.", type: "boolean", defaultValue: false, tunedValue: false },
    ],
  },
  artemis: {
    broker: [
      { key: "journalType", label: "Journal type", help: "Broker journal backend. `aio` usually lowers durable write latency on Linux when it is available.", type: "select", options: [{ value: "nio", label: "nio" }, { value: "aio", label: "aio" }], defaultValue: "nio", tunedValue: "aio", balancedValue: "aio", durableValue: "aio" },
      { key: "globalMaxSizeMb", label: "Global max size MB", help: "Broker in-memory message budget before paging starts. Paging increases latency sharply.", type: "number", min: 64, step: 64, defaultValue: 1024, tunedValue: 1024 },
      { key: "journalBufferTimeoutNs", label: "Journal buffer timeout ns", help: "Maximum time a journal buffer waits before it is flushed. Lower values reduce write wait time and can increase I/O pressure.", type: "number", min: 1000, step: 1000, defaultValue: 3333333, tunedValue: 1000000, throughputValue: 2000000 },
      { key: "journalFileSizeBytes", label: "Journal file size bytes", help: "Journal file size on disk. Larger files reduce file rotation frequency.", type: "number", min: 1048576, step: 1048576, defaultValue: 10485760, tunedValue: 10485760 },
      { key: "journalBufferSizeBytes", label: "Journal buffer size bytes", help: "Size of the journal write buffer. This affects how writes are grouped before flush.", type: "number", min: 65536, step: 65536, defaultValue: 524288, tunedValue: 524288, throughputValue: 1048576 },
      { key: "maxDeliveryAttempts", label: "Max delivery attempts", help: "Maximum deliveries before routing to the DLQ. This mainly affects retry scenarios, not clean runs.", type: "number", min: 1, step: 1, defaultValue: 10, tunedValue: 5 },
      { key: "redeliveryDelayMs", label: "Redelivery delay ms", help: "Delay before a redelivery attempt. Keep it at zero for clean latency runs without retry delay.", type: "number", min: 0, step: 1, defaultValue: 0, tunedValue: 0 },
      { key: "addressFullPolicy", label: "Address full policy", help: "What Artemis does when an address is full. `BLOCK` makes backpressure visible immediately.", type: "select", options: [{ value: "PAGE", label: "PAGE" }, { value: "BLOCK", label: "BLOCK" }, { value: "FAIL", label: "FAIL" }, { value: "DROP", label: "DROP" }], defaultValue: "PAGE", tunedValue: "BLOCK" },
      { key: "pageSizeBytes", label: "Page size bytes", help: "Page size used when messages spill to disk. Smaller pages can make pressure visible sooner.", type: "number", min: 1024, step: 1024, defaultValue: 10485760, tunedValue: 4194304, throughputValue: 10485760 },
      { key: "threadPoolMaxSize", label: "Thread pool max size", help: "Maximum broker worker threads. More threads can help under fan-in load, but too many increase scheduling overhead.", type: "number", min: 1, step: 1, defaultValue: 30, tunedValue: 60, throughputValue: 80 },
    ],
    producer: [],
    consumer: [],
  },
  nats: {
    broker: [
      { key: "maxPayload", label: "Max payload bytes", help: "Maximum message payload size.", type: "number", min: 1024, step: 1024, defaultValue: 1048576, tunedValue: 1048576 },
      { key: "maxMemoryStore", label: "Max memory store MB", help: "Maximum memory for JetStream storage.", type: "number", min: 64, step: 64, defaultValue: 2048, tunedValue: 2048 },
    ],
    producer: [],
    consumer: [],
  },
};

function required(id) {
  const element = document.getElementById(id);
  if (!element) {
    throw new Error(`Missing element #${id}`);
  }
  return element;
}

function optional(id) {
  return document.getElementById(id);
}

const elements = {
  tabs: Array.from(document.querySelectorAll(".tab")),
  panels: Array.from(document.querySelectorAll(".tab-panel")),
  stepperItems: Array.from(document.querySelectorAll("#wizardStepper li")),
  wizardSteps: Array.from(document.querySelectorAll(".wizard-step")),
  benchmarkForm: required("benchmarkForm"),
  brokerSelect: required("brokerSelect"),
  brokerCards: required("brokerCards"),
  brokerIdInput: required("brokerIdInput"),
  configModeSelect: required("configModeSelect"),
  deploymentModeSelect: required("deploymentModeSelect"),
  protocolSelect: required("protocolSelect"),
  configScopeTabs: required("configScopeTabs"),
  configEditor: required("configEditor"),
  scenarioSelect: required("scenarioSelect"),
  scenarioSummaryHint: required("scenarioSummaryHint"),
  rateProfileSelect: required("rateProfileSelect"),
  loadShapeSummaryHint: required("loadShapeSummaryHint"),
  startsAtInput: required("startsAtInput"),
  runNameInput: required("runNameInput"),
  messageRateInput: required("messageRateInput"),
  messageSizeBytesInput: required("messageSizeBytesInput"),
  producersInput: required("producersInput"),
  consumersInput: required("consumersInput"),
  warmupSecondsInput: required("warmupSecondsInput"),
  measurementSecondsInput: required("measurementSecondsInput"),
  cooldownSecondsInput: required("cooldownSecondsInput"),
  windowSummaryValue: required("windowSummaryValue"),
  windowSummaryHint: required("windowSummaryHint"),
  finalRunSummaryCard: required("finalRunSummaryCard"),
  stepBackButton: required("stepBackButton"),
  stepNextButton: required("stepNextButton"),
  startBenchmarkButton: required("startBenchmarkButton"),
  resetAllButton: required("resetAllButton"),
  brokerTuningHidden: required("brokerTuningHidden"),
  scalingSummaryValue: required("scalingSummaryValue"),
  scalingSummaryHint: required("scalingSummaryHint"),
  runList: required("runList"),
  flashMessage: required("flashMessage"),
  serverTime: required("serverTime"),
  runCount: required("runCount"),
  activeCount: required("activeCount"),
  operatorStatus: required("operatorStatus"),
  exportResultsButton: required("exportResultsButton"),
  exportAllResultsButton: required("exportAllResultsButton"),
  importResultsButton: required("importResultsButton"),
  importResultsInput: required("importResultsInput"),
  resultsRefreshButton: optional("resultsRefreshButton"),
  vizRunPicker: required("vizRunPicker"),
  vizSummary: required("vizSummary"),
  reportRunPicker: required("reportRunPicker"),
  latencyCardTitle: required("latencyCardTitle"),
  latencyCardCaption: required("latencyCardCaption"),
  latencyCardMeta: required("latencyCardMeta"),
  percentileCardTitle: required("percentileCardTitle"),
  percentileCardCaption: required("percentileCardCaption"),
  percentileCardMeta: required("percentileCardMeta"),
  distributionCardTitle: required("distributionCardTitle"),
  distributionCardCaption: required("distributionCardCaption"),
  distributionCardMeta: required("distributionCardMeta"),
  spreadCardTitle: required("spreadCardTitle"),
  spreadCardCaption: required("spreadCardCaption"),
  spreadCardMeta: required("spreadCardMeta"),
  throughputCardTitle: required("throughputCardTitle"),
  throughputCardCaption: required("throughputCardCaption"),
  throughputCardMeta: required("throughputCardMeta"),
  resourceCardTitle: required("resourceCardTitle"),
  resourceCardCaption: required("resourceCardCaption"),
  resourceCardMeta: required("resourceCardMeta"),
  latencyChart: required("latencyChart"),
  throughputChart: required("throughputChart"),
  successChart: required("successChart"),
  sizeImpactChart: required("sizeImpactChart"),
  scalingChart: required("scalingChart"),
  resourceVizCard: required("resourceVizCard"),
  resourceChart: required("resourceChart"),
  reportTitleInput: required("reportTitleInput"),
  reportSectionPicker: required("reportSectionPicker"),
  buildReportButton: required("buildReportButton"),
  exportPdfButton: required("exportPdfButton"),
  reportPreview: required("reportPreview"),
};

const chartInstances = {};

function getOrCreateChart(canvasId, config) {
  if (chartInstances[canvasId]) {
    chartInstances[canvasId].destroy();
  }
  const canvas = document.getElementById(canvasId);
  if (!canvas) return null;
  const chart = new Chart(canvas, config);
  chartInstances[canvasId] = chart;
  return chart;
}

function showFlash(message, tone = "success") {
  elements.flashMessage.textContent = message;
  elements.flashMessage.className = `flash ${tone}`;
  window.setTimeout(() => {
    elements.flashMessage.className = "flash hidden";
  }, 2600);
}

function formatDate(value) {
  if (!value) {
    return "-";
  }
  return new Date(value).toLocaleString();
}

function formatNumber(value) {
  return Number(value || 0).toLocaleString();
}

function formatMetric(value, digits = 2) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "-";
  }
  return numeric.toFixed(digits).replace(/\.0+$/, "").replace(/(\.\d*[1-9])0+$/, "$1");
}

function formatBytesCompact(value) {
  const bytes = Number(value || 0);
  if (bytes >= 1024 * 1024) {
    return `${formatMetric(bytes / (1024 * 1024), bytes >= 10 * 1024 * 1024 ? 0 : 1)} MiB`;
  }
  if (bytes >= 1024) {
    return `${formatMetric(bytes / 1024, bytes >= 10 * 1024 ? 0 : 1)} KiB`;
  }
  return `${bytes} B`;
}

function formatBytesPerSecond(value) {
  const bytes = Number(value || 0);
  if (bytes >= 1024 * 1024) {
    return `${formatMetric(bytes / (1024 * 1024), 2)} MiB/s`;
  }
  if (bytes >= 1024) {
    return `${formatMetric(bytes / 1024, 2)} KiB/s`;
  }
  return `${formatMetric(bytes, 0)} B/s`;
}

function formatDuration(seconds) {
  const total = Math.max(0, Number(seconds || 0));
  if (total >= 3600) {
    const hours = Math.floor(total / 3600);
    const minutes = Math.floor((total % 3600) / 60);
    const remaining = total % 60;
    if (!minutes && !remaining) {
      return `${hours}h`;
    }
    if (!remaining) {
      return `${hours}h ${minutes}m`;
    }
    if (!minutes) {
      return `${hours}h ${remaining}s`;
    }
    return `${hours}h ${minutes}m ${remaining}s`;
  }
  if (total < 60) {
    return `${total}s`;
  }
  const minutes = Math.floor(total / 60);
  const remaining = total % 60;
  return remaining ? `${minutes}m ${remaining}s` : `${minutes}m`;
}

function configuredRunDurationSeconds(run) {
  return (
    Number(run?.warmupSeconds || 0) +
    Number(run?.measurementSeconds || 0) +
    Number(run?.cooldownSeconds || 0)
  );
}

function effectiveMeasuredDurationSeconds(run) {
  return Number(run?.metrics?.measurement?.effectiveWindowSeconds || run?.measurementSeconds || 0);
}

function runDurationLabel(run) {
  const configured = configuredRunDurationSeconds(run);
  const effective = hasMeasuredResults(run) ? effectiveMeasuredDurationSeconds(run) : 0;
  const detail = [];
  if (configured > 0) {
    detail.push(`${formatDuration(configured)} total`);
  }
  if (effective > 0) {
    detail.push(`${formatDuration(effective)} effective`);
  }
  return detail.join(", ") || "-";
}

function summarizeRunDurations(runs) {
  if (!runs.length) {
    return "-";
  }
  const configured = runs.map((run) => configuredRunDurationSeconds(run)).filter((value) => value > 0);
  const effective = runs
    .map((run) => (hasMeasuredResults(run) ? effectiveMeasuredDurationSeconds(run) : 0))
    .filter((value) => value > 0);
  const configuredLabel = configured.length && configured.every((value) => value === configured[0])
    ? `${formatDuration(configured[0])} total`
    : configured.length
      ? `${formatDuration(Math.min(...configured))} to ${formatDuration(Math.max(...configured))} total`
      : "";
  const effectiveLabel = effective.length && effective.every((value) => value === effective[0])
    ? `${formatDuration(effective[0])} effective`
    : effective.length
      ? `${formatDuration(Math.min(...effective))} to ${formatDuration(Math.max(...effective))} effective`
      : "";
  return compactList([configuredLabel, effectiveLabel]) || "-";
}

function metricMedian(values) {
  const sorted = values
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value))
    .sort((left, right) => left - right);
  if (!sorted.length) return 0;
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) {
    return (sorted[middle - 1] + sorted[middle]) / 2;
  }
  return sorted[middle];
}

function metricMean(values) {
  const numeric = values.map((value) => Number(value)).filter((value) => Number.isFinite(value));
  if (!numeric.length) return 0;
  return numeric.reduce((sum, value) => sum + value, 0) / numeric.length;
}

function metricMax(values) {
  const numeric = values.map((value) => Number(value)).filter((value) => Number.isFinite(value));
  return numeric.length ? Math.max(...numeric) : 0;
}

function chooseTimeBucket(points) {
  const length = Array.isArray(points) ? points.length : 0;
  if (length >= 3000) return 60;
  if (length >= 1500) return 30;
  if (length >= 600) return 15;
  if (length >= 240) return 10;
  if (length >= 120) return 5;
  return 1;
}

function formatBucketLabel(second, bucketSeconds) {
  const rounded = Math.max(0, Number(second || 0));
  if (bucketSeconds >= 60) {
    const minutes = Math.round(rounded / 60);
    return `${minutes}m`;
  }
  return `${rounded}s`;
}

function bucketTimeseries(points = []) {
  if (!points.length) {
    return {
      bucketSeconds: 1,
      labels: [],
      latencyP50: [],
      latencyP95: [],
      latencyP99: [],
      throughput: [],
      targetRate: [],
      producerRate: [],
      consumerRate: [],
      cpuCores: [],
      memoryGB: [],
    };
  }
  const bucketSeconds = chooseTimeBucket(points);
  if (bucketSeconds <= 1) {
    return {
      bucketSeconds,
      labels: points.map((point) => formatBucketLabel(point.second || 0, bucketSeconds)),
      latencyP50: points.map((point) => Number(point.latencyP50Ms || point.latencyMs || 0)),
      latencyP95: points.map((point) => Number(point.latencyP95Ms || point.latencyMs || 0)),
      latencyP99: points.map((point) => Number(point.latencyP99Ms || point.latencyP95Ms || point.latencyMs || 0)),
      throughput: points.map((point) => Number(point.throughput || 0)),
      targetRate: points.map((point) => Number(point.producerRate || point.throughput || 0)),
      producerRate: points.map((point) => Number(point.producerRate || 0)),
      consumerRate: points.map((point) => Number(point.consumerRate || 0)),
      cpuCores: points.map((point) => (point.cpuCores !== undefined ? Number(point.cpuCores || 0) : null)),
      memoryGB: points.map((point) => (point.memoryMB !== undefined ? Number(point.memoryMB || 0) / 1024 : null)),
    };
  }

  const buckets = new Map();
  for (const point of points) {
    const second = Math.max(0, Number(point.second || 0));
    const key = Math.floor(second / bucketSeconds) * bucketSeconds;
    if (!buckets.has(key)) {
      buckets.set(key, []);
    }
    buckets.get(key).push(point);
  }
  const orderedKeys = Array.from(buckets.keys()).sort((left, right) => left - right);
  return {
    bucketSeconds,
    labels: orderedKeys.map((key) => formatBucketLabel(key, bucketSeconds)),
    latencyP50: orderedKeys.map((key) => metricMedian(buckets.get(key).map((point) => point.latencyP50Ms || point.latencyMs || 0))),
    latencyP95: orderedKeys.map((key) => metricMedian(buckets.get(key).map((point) => point.latencyP95Ms || point.latencyMs || 0))),
    latencyP99: orderedKeys.map((key) => metricMax(buckets.get(key).map((point) => point.latencyP99Ms || point.latencyP95Ms || point.latencyMs || 0))),
    throughput: orderedKeys.map((key) => metricMean(buckets.get(key).map((point) => point.throughput || 0))),
    targetRate: orderedKeys.map((key) => metricMean(buckets.get(key).map((point) => point.producerRate || point.throughput || 0))),
    producerRate: orderedKeys.map((key) => metricMean(buckets.get(key).map((point) => point.producerRate || 0))),
    consumerRate: orderedKeys.map((key) => metricMean(buckets.get(key).map((point) => point.consumerRate || 0))),
    cpuCores: orderedKeys.map((key) => metricMean(buckets.get(key).map((point) => point.cpuCores))),
    memoryGB: orderedKeys.map((key) => metricMean(buckets.get(key).map((point) => point.memoryMB !== undefined ? Number(point.memoryMB || 0) / 1024 : null))),
  };
}

function safeJsonParse(value, fallback = {}) {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function clampLabel(label, max = 14) {
  if (!label || label.length <= max) {
    return label;
  }
  return `${label.slice(0, max - 3)}...`;
}

function summarizeSelectedRunNames(runs, max = 3) {
  if (!runs.length) return "No runs selected";
  const names = runs.map((run) => run.name);
  if (names.length <= max) return names.join(", ");
  return `${names.slice(0, max).join(", ")} +${names.length - max} more`;
}

function compactList(parts) {
  return parts
    .map((part) => String(part || "").trim())
    .filter(Boolean)
    .join(", ");
}

function readPath(object, path, fallback = undefined) {
  if (!object || !path) {
    return fallback;
  }
  const value = path.split(".").reduce((acc, key) => (acc && acc[key] !== undefined ? acc[key] : undefined), object);
  return value === undefined ? fallback : value;
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const body = await response.text();
  if (!response.ok) {
    const parsed = safeJsonParse(body, {});
    throw new Error(parsed.detail || parsed.message || body || `Request failed: ${response.status}`);
  }
  return body ? safeJsonParse(body, {}) : {};
}

function parseDownloadFileName(disposition, fallback = "download.json") {
  const raw = String(disposition || "");
  const utf8Match = raw.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf8Match?.[1]) {
    return decodeURIComponent(utf8Match[1]);
  }
  const match = raw.match(/filename=\"?([^\";]+)\"?/i);
  return match?.[1] || fallback;
}

async function fetchDownload(url, options = {}, fallbackFileName = "download.json") {
  const response = await fetch(url, options);
  if (!response.ok) {
    const body = await response.text();
    const parsed = safeJsonParse(body, {});
    throw new Error(parsed.detail || parsed.message || body || `Request failed: ${response.status}`);
  }
  return {
    blob: await response.blob(),
    fileName: parseDownloadFileName(response.headers.get("Content-Disposition"), fallbackFileName),
  };
}

function triggerBrowserDownload(blob, fileName) {
  const objectUrl = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = objectUrl;
  link.download = fileName;
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.setTimeout(() => URL.revokeObjectURL(objectUrl), 2000);
}

function setActiveTab(tabName) {
  state.activeTab = tabName;
  for (const tab of elements.tabs) {
    const active = tab.dataset.tab === tabName;
    tab.classList.toggle("active", active);
    tab.setAttribute("aria-selected", active ? "true" : "false");
  }
  for (const panel of elements.panels) {
    panel.classList.toggle("active", panel.dataset.panel === tabName);
  }
  if (tabName === "runs" || tabName === "results" || tabName === "reports") {
    refreshData().catch((error) => {
      console.error(error);
    });
  }
  if (tabName === "results") {
    renderVisualizations();
  } else if (tabName === "reports") {
    renderReportPreview();
  }
}

function ensureResultsRefreshButton() {
  if (elements.resultsRefreshButton) {
    return elements.resultsRefreshButton;
  }
  const controls = document.querySelector("[data-panel='results'] .panel-head .controls");
  if (!(controls instanceof HTMLElement)) {
    return null;
  }
  const button = document.createElement("button");
  button.id = "resultsRefreshButton";
  button.className = "button";
  button.type = "button";
  button.textContent = "Refresh";
  controls.insertBefore(button, controls.firstChild);
  elements.resultsRefreshButton = button;
  return button;
}

function brokerMeta() {
  return state.brokers.find((item) => item.id === state.selectedBrokerId) || null;
}

function brokerLabel(brokerId) {
  const broker = state.brokers.find((item) => item.id === brokerId);
  return broker ? broker.label : brokerId;
}

function brokerOperatorState(brokerId = state.selectedBrokerId) {
  const broker = state.brokers.find((item) => item.id === brokerId);
  return (
    state.bootstrapStatus?.operators?.[brokerId] ||
    broker?.operatorStatus || {
      label: broker?.label || brokerId,
      ready: false,
      message: "control plane status unavailable",
    }
  );
}

function brokerReadyForRun(brokerId = state.selectedBrokerId) {
  const broker = state.brokers.find((item) => item.id === brokerId);
  return !!(broker?.runtimeReady || brokerOperatorState(brokerId).ready);
}

function setupPresetMeta(setupId = elements.configModeSelect.value || "latency") {
  return SETUP_PRESETS[setupId] || SETUP_PRESETS.latency;
}

function selectedSetupId() {
  return elements.configModeSelect.value || "latency";
}

function effectiveConfigMode(setupId = elements.configModeSelect.value || "latency") {
  return setupPresetMeta(setupId).configMode;
}

function setupLabel(setupId = elements.configModeSelect.value || "latency") {
  return setupPresetMeta(setupId).label;
}

function loadProfileValue() {
  return String(elements.rateProfileSelect.value || "constant").trim().toLowerCase() || "constant";
}

function peakMessageRateValue(value = elements.messageRateInput.value || 1, profile = loadProfileValue()) {
  const baseRate = Math.max(1, Number(value || 1));
  if (profile === "constant") {
    return baseRate;
  }
  return Math.max(baseRate, baseRate * 2);
}

function loadProfileName(profile = loadProfileValue()) {
  const normalized = String(profile || "constant").trim().toLowerCase();
  if (normalized === "constant") return "Steady";
  if (normalized === "ramp-up") return "Ramp up";
  if (normalized === "ramp-down") return "Ramp down";
  if (normalized === "step-up") return "Step up";
  if (normalized === "burst") return "Burst";
  return normalized.replaceAll("-", " ");
}

function loadProfileLabel(
  profile = loadProfileValue(),
  baseRate = Number(elements.messageRateInput.value || 1),
  peakRate = peakMessageRateValue(baseRate, profile)
) {
  const base = Math.max(1, Number(baseRate || 1));
  const peak = Math.max(base, Number(peakRate || base));
  const name = loadProfileName(profile);
  if (profile === "constant") {
    return `${name} ${formatNumber(base)} msg/s`;
  }
  return `${name} ${formatNumber(base)} -> ${formatNumber(peak)} msg/s`;
}

function runLoadProfileLabel(run) {
  const profile = runLoadProfile(run);
  const baseRate = Math.max(1, Number(run?.messageRate || 1));
  return loadProfileLabel(profile.rateProfileKind, baseRate, profile.peakMessageRate || baseRate);
}

function rateProfileTarget(profileId) {
  const profile = state.rateProfiles[profileId];
  if (!profile) return null;
  if (profile.targetMessagesPerSecond) {
    return Number(profile.targetMessagesPerSecond);
  }
  if (Array.isArray(profile.steps) && profile.steps.length) {
    return Number(profile.steps[0]?.targetMessagesPerSecond || profile.steps[profile.steps.length - 1]?.targetMessagesPerSecond || 0) || null;
  }
  if (profile.maxMessagesPerSecond) {
    return Number(profile.maxMessagesPerSecond);
  }
  return null;
}

function loadProfileFromRateProfile(profileId) {
  const profile = state.rateProfiles[profileId];
  if (!profile) {
    return { rateProfileKind: "constant", peakMessageRate: null };
  }
  if (profile.kind === "ramp") {
    return {
      rateProfileKind: "ramp-up",
      peakMessageRate: Number(profile.maxMessagesPerSecond || profile.targetMessagesPerSecond || 0) || null,
    };
  }
  if (profile.kind === "step") {
    const steps = Array.isArray(profile.steps) ? profile.steps : [];
    const base = Number(steps[0]?.targetMessagesPerSecond || profile.targetMessagesPerSecond || 0) || null;
    const peak = Number(steps.reduce((max, step) => Math.max(max, Number(step?.targetMessagesPerSecond || 0)), 0)) || null;
    return {
      rateProfileKind: String(profileId).toLowerCase().includes("burst") ? "burst" : "step-up",
      peakMessageRate: Math.max(base || 0, peak || 0) || null,
    };
  }
  return {
    rateProfileKind: "constant",
    peakMessageRate: Number(profile.targetMessagesPerSecond || 0) || null,
  };
}

function presetOptionLabel(preset) {
  const producers = Number(preset.producers || 1);
  const consumers = Number(preset.consumers || 1);
  const size = formatBytesCompact(preset.payloadSizeBytes || 0);
  const flow = loadProfileFromRateProfile(preset.rateProfile);
  const target = rateProfileTarget(preset.rateProfile) || 1;
  const category = String(preset.categoryLabel || preset.category || "").trim();
  const topology = `${producers}p/${consumers}c`;
  const name = String(preset.name || "").trim();
  const detail = compactList([
    topology,
    size,
    loadProfileLabel(flow.rateProfileKind, target, flow.peakMessageRate || target),
    category,
  ]);
  return name ? `${name} - ${detail}` : detail;
}

function profileLabel(mode) {
  return mode === "optimized" ? "Performance" : "Standard";
}

function runSetupLabel(run) {
  const setupId = run?.brokerTuning?.setupPreset;
  if (setupId && SETUP_PRESETS[setupId]) {
    return setupLabel(setupId);
  }
  return profileLabel(run?.configMode);
}

function deploymentLabel(mode) {
  return mode === "ha" ? "3-node cluster" : "Single node";
}

function currentTargetReplicas() {
  return elements.deploymentModeSelect.value === "ha" ? 3 : 1;
}

function transportOptionsObject(value) {
  if (!value) return {};
  if (typeof value === "string") return safeJsonParse(value, {});
  if (typeof value === "object") return value;
  return {};
}

function runLoadProfile(run) {
  const options = transportOptionsObject(run?.transportOptions);
  const rateProfileKind = String(
    options.rateProfileKind || options.rateProfile || "constant"
  ).toLowerCase();
  const peakMessageRate = Number(
    options.peakMessageRate ||
      run?.metrics?.summary?.loadProfile?.peakMessageRate ||
      run?.messageRate ||
      0
  ) || Number(run?.messageRate || 0);
  return {
    rateProfileKind,
    peakMessageRate,
  };
}

function statusClass(status) {
  return `status-${String(status || "unknown").toLowerCase()}`;
}

function currentProfile() {
  const mode = effectiveConfigMode(elements.configModeSelect.value || "latency");
  return state.brokerProfiles.find(
    (profile) =>
      profile.brokerId === state.selectedBrokerId &&
      String(profile.mode).toLowerCase() === String(mode)
  );
}

function profileForMode(mode) {
  return state.brokerProfiles.find(
    (profile) =>
      profile.brokerId === state.selectedBrokerId &&
      String(profile.mode).toLowerCase() === String(mode)
  );
}

function profileForBroker(brokerId, mode = effectiveConfigMode(elements.configModeSelect.value || "latency")) {
  return state.brokerProfiles.find(
    (profile) =>
      profile.brokerId === brokerId &&
      String(profile.mode).toLowerCase() === String(mode).toLowerCase()
  );
}

function scenarioBrokerTarget(preset, brokerId = state.selectedBrokerId) {
  if (!preset || typeof preset !== "object") {
    return {};
  }
  const brokerTargets = preset.brokerTargets || {};
  return brokerTargets[brokerId] || {};
}

function defaultResourceConfigForBroker(
  brokerId = state.selectedBrokerId,
  mode = effectiveConfigMode(selectedSetupId()),
  deploymentMode = elements.deploymentModeSelect.value || "normal",
  setupId = selectedSetupId()
) {
  const profile = profileForBroker(brokerId, mode) || {};
  const deployment = profile.deployment || {};
  const requests = deployment.resources?.requests || {};
  const limits = deployment.resources?.limits || {};
  const storage = deployment.storage || {};
  const preset = profile.resourcePresets?.[setupId] || {};
  return {
    cpuRequest: String(preset.cpuRequest || requests.cpu || limits.cpu || (brokerId === "kafka" ? "1500m" : "2")),
    cpuLimit: String(preset.cpuLimit || limits.cpu || requests.cpu || (brokerId === "kafka" ? "1500m" : "2")),
    memoryRequest: String(preset.memoryRequest || requests.memory || limits.memory || (brokerId === "kafka" ? "3Gi" : "4Gi")),
    memoryLimit: String(preset.memoryLimit || limits.memory || requests.memory || (brokerId === "kafka" ? "3Gi" : "4Gi")),
    storageSize: String(preset.storageSize || storage.size || "20Gi"),
    storageClassName: String(preset.storageClassName || storage.className || ""),
    replicas: deploymentMode === "ha" ? 3 : Number(deployment.replicas || 1),
  };
}

function recommendedResourceConfigForBroker(
  brokerId = state.selectedBrokerId,
  defaults = defaultResourceConfigForBroker(brokerId)
) {
  const preset = currentPreset();
  const target = scenarioBrokerTarget(preset, brokerId);
  const recommended = target.resourceConfig || preset?.resourceConfig || {};
  return {
    ...defaults,
    ...recommended,
    replicas: Math.max(1, Number(recommended.replicas || defaults.replicas || 1)),
  };
}

function buildResourceConfigDraft(defaults, previous = {}) {
  const custom = { ...(previous.__custom || {}) };
  const recommended = { ...(previous.__recommended || {}) };
  const next = {
    __custom: custom,
    __recommended: recommended,
    __replicasCustom: Boolean(previous.__replicasCustom),
  };
  for (const key of RESOURCE_CONFIG_KEYS) {
    const recommendedValue = recommended[key] ?? defaults[key] ?? "";
    const hasCustomValue = Object.prototype.hasOwnProperty.call(custom, key) && Boolean(custom[key]);
    next[key] = hasCustomValue ? String(previous[key] ?? "") : String(recommendedValue ?? "");
  }
  const recommendedReplicas = Number(recommended.replicas || defaults.replicas || 1);
  next.replicas = next.__replicasCustom
    ? Math.max(1, Number(previous.replicas || recommendedReplicas || 1))
    : Math.max(1, recommendedReplicas);
  return next;
}

function currentResourceConfig(brokerId = state.selectedBrokerId) {
  const defaults = defaultResourceConfigForBroker(brokerId);
  const recommended = recommendedResourceConfigForBroker(brokerId, defaults);
  const previous = state.resourceConfigByBroker[brokerId] || {};
  previous.__recommended = {
    ...(previous.__recommended || {}),
    ...recommended,
  };
  state.resourceConfigByBroker[brokerId] = buildResourceConfigDraft(
    defaults,
    previous
  );
  return state.resourceConfigByBroker[brokerId];
}

function resetResourceConfigForBroker(brokerId = state.selectedBrokerId) {
  const defaults = defaultResourceConfigForBroker(brokerId);
  state.resourceConfigByBroker[brokerId] = buildResourceConfigDraft(
    defaults,
    { __recommended: recommendedResourceConfigForBroker(brokerId, defaults) }
  );
}

function resourceConfigPayload(brokerId = state.selectedBrokerId) {
  const config = currentResourceConfig(brokerId);
  const payload = {};
  for (const key of RESOURCE_CONFIG_KEYS) {
    payload[key] = config[key];
  }
  if (config.__replicasCustom) {
    payload.replicas = Math.max(1, Number(config.replicas || 1));
  }
  return payload;
}

function resourceConfigSummary(config = currentResourceConfig()) {
  const parts = [];
  if (config.cpuRequest || config.cpuLimit) {
    parts.push(`CPU ${config.cpuRequest || "-"} / ${config.cpuLimit || "-"}`);
  }
  if (config.memoryRequest || config.memoryLimit) {
    parts.push(`Memory ${config.memoryRequest || "-"} / ${config.memoryLimit || "-"}`);
  }
  if (config.storageSize) {
    parts.push(`Storage ${config.storageSize}`);
  }
  if (config.storageClassName) {
    parts.push(`Class ${config.storageClassName}`);
  }
  if (Object.prototype.hasOwnProperty.call(config, "__replicasCustom") && !config.__replicasCustom) {
    parts.push("Replicas auto");
  } else if (config.replicas) {
    parts.push(`Replicas ${config.replicas}`);
  }
  return compactList(parts) || "-";
}

function reportResourceSummary(config = currentResourceConfig()) {
  const cpu = String(config.cpuLimit || config.cpuRequest || "").trim();
  const memory = String(config.memoryLimit || config.memoryRequest || "").trim();
  const storage = String(config.storageSize || "").trim();
  const replicas = Number(config.replicas || 0);
  const parts = [];
  if (cpu) parts.push(`${cpu} CPU`);
  if (memory) parts.push(memory);
  if (replicas) parts.push(`x${replicas}`);
  if (storage) parts.push(storage);
  return compactList(parts) || "-";
}

function brokerRuntimeDescriptor(broker = brokerMeta()) {
  if (!broker) {
    return "Unknown";
  }
  return broker.operatorVersion || "Managed";
}

function getFieldDefinitions(scope = state.configScope, brokerId = state.selectedBrokerId) {
  return CONFIG_FIELDS[brokerId]?.[scope] || [];
}

function profileValueForField(field, profile, brokerId) {
  if (field.key === "lingerMs") return readPath(profile, "clientDefaults.producer.lingerMs", field.defaultValue);
  if (field.key === "batchSizeBytes") return readPath(profile, "clientDefaults.producer.batchSizeBytes", field.defaultValue);
  if (field.key === "compressionType") return readPath(profile, "clientDefaults.producer.compressionType", field.defaultValue);
  if (field.key === "acks") return readPath(profile, "semanticMapping.confirmed.producerAcks", field.defaultValue);
  if (field.key === "fetchMinBytes") return readPath(profile, "clientDefaults.consumer.fetchMinBytes", field.defaultValue);
  if (field.key === "fetchMaxWaitMs") return readPath(profile, "clientDefaults.consumer.fetchMaxWaitMs", field.defaultValue);
  if (field.key === "maxPollRecords") return readPath(profile, "clientDefaults.consumer.maxPollRecords", field.defaultValue);
  if (field.key === "enableAutoCommit") return readPath(profile, "clientDefaults.consumer.enableAutoCommit", field.defaultValue);
  if (brokerId === "rabbitmq" && field.key === "defaultQueueType") return readPath(profile, "semanticMapping.persistent.queueType", field.defaultValue);
  if (brokerId === "rabbitmq" && field.key === "publisherConfirms") return readPath(profile, "clientDefaults.producer.publisherConfirms", field.defaultValue);
  if (brokerId === "rabbitmq" && field.key === "mandatory") return readPath(profile, "clientDefaults.producer.mandatory", field.defaultValue);
  if (brokerId === "rabbitmq" && field.key === "prefetch") return readPath(profile, "clientDefaults.consumer.prefetch", field.defaultValue);
  if (brokerId === "rabbitmq" && field.key === "autoAck") return readPath(profile, "clientDefaults.consumer.autoAck", field.defaultValue);
  if (brokerId === "artemis" && field.key === "journalType") return readPath(profile, "deployment.journalType", field.defaultValue);
  return field.defaultValue;
}

function interpolateValue(field, fromValue, toValue) {
  if (!Number.isFinite(Number(fromValue)) || !Number.isFinite(Number(toValue))) {
    return toValue;
  }
  const raw = (Number(fromValue) + Number(toValue)) / 2;
  const step = Number(field.step || 1);
  if (!Number.isFinite(step) || step <= 0) {
    return raw;
  }
  return Math.round(raw / step) * step;
}

function valueForSetup(field, profileValue, setupId) {
  if (setupId === "defaults") {
    return profileValue;
  }
  if (setupId === "latency") {
    return field.tunedValue ?? profileValue;
  }
  if (setupId === "throughput") {
    if (field.throughputValue !== undefined) {
      return field.throughputValue;
    }
    if (field.type === "number" && field.tunedValue !== undefined) {
      return field.tunedValue;
    }
    return field.tunedValue ?? profileValue;
  }
  if (setupId === "balanced") {
    if (field.balancedValue !== undefined) {
      return field.balancedValue;
    }
    if (field.type === "number" && field.tunedValue !== undefined) {
      return interpolateValue(field, field.defaultValue, field.tunedValue);
    }
    return field.tunedValue ?? profileValue;
  }
  if (setupId === "durable") {
    if (field.durableValue !== undefined) {
      return field.durableValue;
    }
    return profileValue;
  }
  return profileValue;
}

function buildTuningModel(setupId = elements.configModeSelect.value || "latency") {
  const profile = profileForMode(effectiveConfigMode(setupId));
  const brokerId = state.selectedBrokerId;
  const presetMeta = setupPresetMeta(setupId);
  const nextModel = {
    profileId: profile?.id || null,
    profileMode: presetMeta.configMode,
    setupPreset: setupId,
    tuningPreset: presetMeta.tuningPreset,
    broker: {},
    producer: {},
    consumer: {},
  };

  for (const scope of ["broker", "producer", "consumer"]) {
    for (const field of getFieldDefinitions(scope, brokerId)) {
      const profileValue = profileValueForField(field, profile, brokerId);
      const value = valueForSetup(field, profileValue, setupId);
      nextModel[scope][field.key] = value;
    }
  }
  return nextModel;
}

function applyModePreset(setupId) {
  state.tuningPresetKind = setupPresetMeta(setupId).tuningPreset;
  state.tuningModel = buildTuningModel(setupId);
  renderConfigEditor();
  syncTuningHiddenInput();
}

function syncTuningHiddenInput() {
  elements.brokerTuningHidden.value = JSON.stringify(state.tuningModel);
}

function renderBrokerCards() {
  elements.brokerSelect.innerHTML = state.brokers
    .map((broker) => {
      const brokerReady = broker.runtimeReady || broker.operatorStatus?.ready;
      const brokerStatus = brokerReady ? "" : " (unavailable)";
      return `<option value="${broker.id}" ${broker.id === state.selectedBrokerId ? "selected" : ""}>${broker.label}${brokerStatus}</option>`;
    })
    .join("");
  elements.brokerSelect.value = state.selectedBrokerId;
  elements.brokerCards.innerHTML = "";
}

function renderProtocolOptions() {
  const broker = brokerMeta();
  elements.protocolSelect.innerHTML = "";
  if (!broker) {
    return;
  }
  const selected = state.protocolByBroker[state.selectedBrokerId] || broker.defaultProtocol || broker.protocolOptions[0];
  const effective = broker.protocolOptions.includes(selected) ? selected : broker.defaultProtocol;
  for (const protocol of broker.protocolOptions) {
    const option = document.createElement("option");
    option.value = protocol;
    option.textContent = protocol;
    option.selected = protocol === effective;
    elements.protocolSelect.appendChild(option);
  }
  elements.protocolSelect.value = effective;
  state.protocolByBroker[state.selectedBrokerId] = effective;
}

function visiblePresets() {
  return state.presets;
}

function renderPresetOptions() {
  elements.scenarioSelect.innerHTML = `<option value="">Manual setup</option>`;
  for (const preset of visiblePresets()) {
    const option = document.createElement("option");
    option.value = preset.id;
    option.textContent = presetOptionLabel(preset);
    elements.scenarioSelect.appendChild(option);
  }
}

function currentPreset() {
  const presetId = String(elements.scenarioSelect.value || "").trim();
  return visiblePresets().find((item) => item.id === presetId) || null;
}

function routeLabelForPreset(preset) {
  const intent = String(preset?.destinationIntent || "").trim().toLowerCase();
  if (intent === "fanout-pubsub") return "Fanout route";
  if (intent === "consumer-group") return "Shared route";
  if (intent === "single-destination") return "Single route";
  return "Manual route";
}

function updateScenarioSummary() {
  const preset = currentPreset();
  if (!preset) {
    elements.scenarioSummaryHint.textContent = "";
    return;
  }
  const targetConfig = scenarioBrokerTarget(preset);
  const flow = loadProfileFromRateProfile(preset.rateProfile);
  const baseRate = rateProfileTarget(preset.rateProfile) || Number(elements.messageRateInput.value || 1);
  const name = String(preset.name || "").trim();
  const setupId = targetConfig.setupPreset || preset.setupPreset || elements.configModeSelect.value || "latency";
  const deploymentMode = targetConfig.deploymentMode || preset.deploymentMode || elements.deploymentModeSelect.value || "normal";
  const summary = compactList([
    `${preset.producers} producer${preset.producers > 1 ? "s" : ""}`,
    `${preset.consumers} consumer${preset.consumers > 1 ? "s" : ""}`,
    `${formatBytesCompact(preset.payloadSizeBytes || 0)} payload`,
    loadProfileLabel(flow.rateProfileKind, baseRate, flow.peakMessageRate || baseRate),
    setupLabel(setupId),
    deploymentLabel(deploymentMode),
  ]);
  elements.scenarioSummaryHint.textContent = name ? `${name}. ${summary}` : summary;
}

function applySetup(presetId) {
  const preset = visiblePresets().find((item) => item.id === presetId);
  if (!preset) {
    resetResourceConfigForBroker();
    renderResourceConfig();
    updateWizardState();
    return;
  }
  const targetConfig = scenarioBrokerTarget(preset);
  const setupId = targetConfig.setupPreset || preset.setupPreset || elements.configModeSelect.value || "latency";
  const deploymentMode = targetConfig.deploymentMode || preset.deploymentMode || elements.deploymentModeSelect.value || "normal";
  elements.configModeSelect.value = setupId;
  elements.deploymentModeSelect.value = deploymentMode;
  elements.producersInput.value = String(preset.producers || 1);
  elements.consumersInput.value = String(preset.consumers || 1);
  elements.messageSizeBytesInput.value = String(preset.payloadSizeBytes || 1024);
  elements.warmupSecondsInput.value = String(preset.warmupSeconds || elements.warmupSecondsInput.value || 10);
  elements.measurementSecondsInput.value = String(preset.measurementSeconds || elements.measurementSecondsInput.value || 60);
  elements.cooldownSecondsInput.value = String(preset.cooldownSeconds || elements.cooldownSecondsInput.value || 10);
  const target = rateProfileTarget(preset.rateProfile);
  if (target) {
    elements.messageRateInput.value = String(target);
  }
  const loadProfile = loadProfileFromRateProfile(preset.rateProfile);
  elements.rateProfileSelect.value = loadProfile.rateProfileKind;
  if (targetConfig.protocol && brokerMeta()?.protocolOptions?.includes(targetConfig.protocol)) {
    elements.protocolSelect.value = targetConfig.protocol;
    state.protocolByBroker[state.selectedBrokerId] = targetConfig.protocol;
  }
  applyModePreset(setupId);
  const tuningOverrides = targetConfig.tuning || preset.tuning || {};
  for (const scope of ["broker", "producer", "consumer"]) {
    if (tuningOverrides[scope] && typeof tuningOverrides[scope] === "object") {
      state.tuningModel[scope] = {
        ...(state.tuningModel[scope] || {}),
        ...tuningOverrides[scope],
      };
    }
  }
  renderConfigEditor();
  syncTuningHiddenInput();
  resetResourceConfigForBroker();
  renderBrokerCards();
  renderResourceConfig();
  updateWizardState();
}

function fieldInputHtml(scope, field, value) {
  const inputId = `config-${scope}-${field.key}`;
  if (field.type === "select") {
    const options = (field.options || [])
      .map((option) => `<option value="${option.value}" ${String(value) === String(option.value) ? "selected" : ""}>${option.label}</option>`)
      .join("");
    return `<select id="${inputId}" data-scope="${scope}" data-key="${field.key}">${options}</select>`;
  }
  if (field.type === "boolean") {
    return `<label class="toggle"><input id="${inputId}" type="checkbox" data-scope="${scope}" data-key="${field.key}" ${value ? "checked" : ""} /><span>${value ? "On" : "Off"}</span></label>`;
  }
  return `<input id="${inputId}" type="${field.type || "number"}" data-scope="${scope}" data-key="${field.key}" value="${value}" ${
    field.min !== undefined ? `min="${field.min}"` : ""
  } ${field.max !== undefined ? `max="${field.max}"` : ""} ${field.step !== undefined ? `step="${field.step}"` : ""} />`;
}

function renderConfigEditor() {
  const availableScopes = ["broker", "producer", "consumer"].filter(
    (scopeName) => getFieldDefinitions(scopeName).length > 0
  );
  if (!availableScopes.includes(state.configScope)) {
    state.configScope = availableScopes[0] || "broker";
  }
  const scope = state.configScope;
  const fields = getFieldDefinitions(scope);
  const scopeModel = state.tuningModel[scope] || {};
  elements.configEditor.innerHTML = fields
    .map(
      (field) => `
        <label class="config-field">
          <span>
            ${field.label}
            <button type="button" class="info-dot" data-tip="${field.help}">i</button>
          </span>
          ${fieldInputHtml(scope, field, scopeModel[field.key] ?? field.defaultValue)}
        </label>
      `
    )
    .join("");

  for (const button of elements.configScopeTabs.querySelectorAll(".scope-tab")) {
    const hasFields = getFieldDefinitions(button.dataset.scope).length > 0;
    button.disabled = !hasFields;
    button.classList.toggle("hidden", !hasFields);
    button.classList.toggle("active", button.dataset.scope === scope);
  }
}

function updateTuningValue(event) {
  const target = event.target;
  const scope = target.dataset.scope;
  const key = target.dataset.key;
  if (!scope || !key) return;
  let value;
  if (target.type === "checkbox") {
    value = target.checked;
    target.parentElement.querySelector("span").textContent = value ? "On" : "Off";
  } else if (target.type === "number") {
    value = Number(target.value);
  } else {
    value = target.value;
  }
  state.tuningModel[scope][key] = value;
  syncTuningHiddenInput();
  renderConfigEditor();
  renderBrokerCards();
  renderFinalRunSummary();
}

function updateScalingSummary() {
  const producers = Math.max(1, Number(elements.producersInput.value || 1));
  const consumers = Math.max(1, Number(elements.consumersInput.value || 1));
  const total = producers + consumers;
  const preset = currentPreset();
  elements.scalingSummaryValue.textContent = `${producers} producer${producers > 1 ? "s" : ""} + ${consumers} consumer${consumers > 1 ? "s" : ""} = ${total} workers`;
  elements.scalingSummaryHint.textContent = compactList([
    deploymentLabel(elements.deploymentModeSelect.value),
    routeLabelForPreset(preset),
  ]);
}

function updateWindowSummary() {
  const warmup = Number(elements.warmupSecondsInput.value || 0);
  const measure = Number(elements.measurementSecondsInput.value || 0);
  const cooldown = Number(elements.cooldownSecondsInput.value || 0);
  const total = warmup + measure + cooldown;
  const startValue = elements.startsAtInput.value;
  const totalLabel = `${formatDuration(total)} total`;

  if (!startValue) {
    elements.windowSummaryValue.textContent = totalLabel;
    elements.windowSummaryHint.textContent = compactList([
      `Warmup ${formatDuration(warmup)}`,
      `Measure ${formatDuration(measure)}`,
      `Cooldown ${formatDuration(cooldown)}`,
      "Start now",
    ]);
    return;
  }

  const start = new Date(startValue);
  const end = new Date(start.getTime() + total * 1000);
  elements.windowSummaryValue.textContent = totalLabel;
  elements.windowSummaryHint.textContent = compactList([
    `Warmup ${formatDuration(warmup)}`,
    `Measure ${formatDuration(measure)}`,
    `Cooldown ${formatDuration(cooldown)}`,
    `${start.toLocaleString()} to ${end.toLocaleTimeString()}`,
  ]);
}

function updateLoadShapeSummary() {
  const baseRate = Math.max(1, Number(elements.messageRateInput.value || 1));
  const profile = loadProfileValue();
  const peakRate = peakMessageRateValue(baseRate, profile);
  elements.loadShapeSummaryHint.textContent = loadProfileLabel(profile, baseRate, peakRate);
}

function blockingRun() {
  return state.runs.find((run) =>
    ["preparing", "scheduled", "warmup", "measuring", "cooldown", "cleaning"].includes(run.status)
  ) || null;
}

function renderFinalRunSummary() {
  const total =
    Number(elements.warmupSecondsInput.value || 0) +
    Number(elements.measurementSecondsInput.value || 0) +
    Number(elements.cooldownSecondsInput.value || 0);
  const activeRun = blockingRun();
  const startValue = elements.startsAtInput.value;
  const startLabel = startValue ? new Date(startValue).toLocaleString() : "Immediate";
  const producers = Math.max(1, Number(elements.producersInput.value || 1));
  const consumers = Math.max(1, Number(elements.consumersInput.value || 1));
  const measure = Number(elements.measurementSecondsInput.value || 0);
  const warmup = Number(elements.warmupSecondsInput.value || 0);
  const cooldown = Number(elements.cooldownSecondsInput.value || 0);
  elements.finalRunSummaryCard.innerHTML = `
    <span>Recap</span>
    <strong>${brokerLabel(state.selectedBrokerId)}</strong>
    <p class="hint">${compactList([setupLabel(elements.configModeSelect.value), deploymentLabel(elements.deploymentModeSelect.value), elements.protocolSelect.value])}</p>
    <p class="hint">${compactList([loadProfileLabel(), `${formatBytesCompact(elements.messageSizeBytesInput.value)} payload`, `${producers} producer${producers > 1 ? "s" : ""}`, `${consumers} consumer${consumers > 1 ? "s" : ""}`])}</p>
    <p class="hint">${compactList([`${formatDuration(total)} total`, `Measure ${formatDuration(measure)}`, `Warmup ${formatDuration(warmup)}`, `Cooldown ${formatDuration(cooldown)}`, `Start ${startLabel}`])}</p>
    ${
      activeRun
        ? `<p class="hint">Run in progress: ${activeRun.name}</p>`
        : ""
    }
  `;
}

function numericInputValid(input, min = 0) {
  const value = Number(input.value);
  return Number.isFinite(value) && value >= min;
}

function wizardReadiness() {
  const step1 =
    !!state.selectedBrokerId &&
    brokerReadyForRun(state.selectedBrokerId) &&
    !!elements.configModeSelect.value &&
    !!elements.deploymentModeSelect.value &&
    !!elements.protocolSelect.value &&
    !!elements.brokerTuningHidden.value;
  const step2 =
    step1 &&
    numericInputValid(elements.messageRateInput, 1) &&
    numericInputValid(elements.messageSizeBytesInput, 1) &&
    numericInputValid(elements.producersInput, 1) &&
    numericInputValid(elements.consumersInput, 1) &&
    numericInputValid(elements.warmupSecondsInput, 0) &&
    numericInputValid(elements.measurementSecondsInput, 1) &&
    numericInputValid(elements.cooldownSecondsInput, 0);
  const step3 = step2 && elements.runNameInput.value.trim().length > 0;
  return { step1, step2, step3 };
}

function unlockedWizardStep(readiness) {
  if (!readiness.step1) return 1;
  if (!readiness.step2) return 2;
  return 3;
}

function setWizardStep(step) {
  const readiness = wizardReadiness();
  const unlocked = unlockedWizardStep(readiness);
  state.wizardStep = Math.max(1, Math.min(step, unlocked));
  renderWizardState();
}

function renderWizardState() {
  const readiness = wizardReadiness();
  const unlocked = unlockedWizardStep(readiness);
  const activeRun = blockingRun();
  if (state.wizardStep > unlocked) {
    state.wizardStep = unlocked;
  }

  for (const item of elements.stepperItems) {
    const step = Number(item.dataset.step);
    item.classList.toggle("active", step === state.wizardStep);
    item.classList.toggle("locked", step > unlocked);
  }

  for (const section of elements.wizardSteps) {
    const step = Number(section.dataset.step);
    section.classList.toggle("active", step === state.wizardStep);
    section.classList.toggle("locked", step > unlocked);
  }

  elements.stepBackButton.disabled = state.wizardStep <= 1;
  elements.stepNextButton.disabled = state.wizardStep >= unlocked;
  elements.startBenchmarkButton.disabled = !readiness.step3 || !!activeRun;
  elements.resetAllButton.disabled = !!activeRun;
}

function eventsForRun(runId) {
  return state.events
    .filter((event) => event.run_id === runId)
    .sort((left, right) => new Date(left.created_at) - new Date(right.created_at));
}

function latestRunEvent(runId) {
  const items = eventsForRun(runId);
  return items.length ? items[items.length - 1] : null;
}

function failureEventForRun(runId) {
  const items = eventsForRun(runId).filter((event) => /(?:failed|error)$/i.test(String(event.event_type || "")));
  return items.length ? items[items.length - 1] : null;
}

function hasMeasuredResults(run) {
  return Number(run?.metrics?.summary?.endToEndLatencyMs?.count || 0) > 0;
}

function compactMessage(message, max = 180) {
  const text = String(message || "").replace(/\s+/g, " ").trim();
  if (!text || text.length <= max) {
    return text;
  }
  return `${text.slice(0, max - 3)}...`;
}

function friendlyRunMessage(message) {
  const text = String(message || "").replace(/\s+/g, " ").trim();
  if (!text) {
    return "Waiting to start.";
  }
  if (text.includes("failed before start")) {
    if (text.includes("producer")) {
      return "Producer startup failed before measured traffic started.";
    }
    if (text.includes("consumer")) {
      return "Consumer startup failed before measured traffic started.";
    }
    return "Benchmark startup failed before measured traffic started.";
  }
  if (text.includes("terminated before start")) {
    return "A benchmark job terminated before measured traffic started.";
  }
  return compactMessage(text);
}

function stageDefinitions() {
  return [
    { label: "Topology", keys: ["topology-started", "prepare-started", "namespace-ready"] },
    { label: "Broker", keys: ["broker-waiting", "broker-ready"] },
    { label: "Producer", keys: ["producer-running"] },
    { label: "Consumer", keys: ["consumer-running"] },
    { label: "Test", keys: ["test-started", "warmup-started", "measurement-started", "cooldown-started", "test-finished"] },
    { label: "Cleanup", keys: ["cleanup-started", "topology-destroyed"] },
  ];
}

function stageStateForRun(run) {
  const types = new Set(eventsForRun(run.id).map((event) => event.event_type));
  const definitions = stageDefinitions();
  const finished = run.status === "completed" || run.status === "stopped";
  const result = definitions.map((stage) => ({
    label: stage.label,
    done: stage.keys.some((key) => types.has(key)),
    active: false,
  }));

  result[0].done = result[0].done || ["preparing", "scheduled", "warmup", "measuring", "cooldown", "finalizing", "cleaning", "completed", "stopped"].includes(run.status);
  result[1].done = result[1].done || !!run.topologyReadyAt || !!run.executionStartedAt || ["cooldown", "finalizing", "cleaning", "completed", "stopped"].includes(run.status);
  result[2].done = result[2].done || !!run.executionStartedAt || ["cooldown", "finalizing", "cleaning", "completed", "stopped"].includes(run.status);
  result[3].done = result[3].done || !!run.executionStartedAt || ["cooldown", "finalizing", "cleaning", "completed", "stopped"].includes(run.status);
  result[4].done = result[4].done || !!run.completedAt || !!run.stoppedAt || ["finalizing", "cleaning", "completed", "stopped"].includes(run.status);
  result[5].done = result[5].done || !!run.topologyDeletedAt;

  if (finished) {
    result.forEach((stage) => {
      stage.done = true;
      stage.active = false;
    });
    return result;
  }

  const firstPending = result.findIndex((stage) => !stage.done);
  if (firstPending >= 0) {
    result[firstPending].active = true;
  } else if (!finished && result.length) {
    result[result.length - 1].active = true;
  }
  return result;
}

function progressPercentForRun(run) {
  const stages = stageStateForRun(run);
  const completed = stages.filter((stage) => stage.done).length;
  const total = stages.length;
  let percent = (completed / total) * 100;
  if (run.executionStartedAt && !run.completedAt) {
    percent = Math.max(percent, 66 + (Number(run.progress?.runPercent || 0) * 0.22));
  }
  if (run.topologyDeletedAt && run.status === "completed") {
    percent = 100;
  }
  return Math.max(5, Math.min(100, percent));
}

function formatRelativeWindow(fromIso, toIso) {
  if (!fromIso || !toIso) {
    return "";
  }
  const deltaSeconds = Math.max(0, Math.round((new Date(toIso) - new Date(fromIso)) / 1000));
  return formatDuration(deltaSeconds);
}

function currentPhaseLabel(run) {
  const status = String(run.status || "").toLowerCase();
  if (status === "scheduled" || status === "preparing") return "Preparing";
  if (status === "warmup") return "Warmup";
  if (status === "measuring") return "Measuring";
  if (status === "cooldown") return "Cooldown";
  if (status === "finalizing") return "Finalizing";
  if (status === "cleaning") return "Cleanup";
  if (status === "completed") return "Completed";
  if (status === "stopped") return "Stopped";
  return "Queued";
}

function runTimeProgressSummary(run) {
  const startAt = run.executionStartedAt || run.startsAt;
  const timeline = [];
  if (startAt) {
    timeline.push(`Start ${formatDate(startAt)}`);
  }
  if (run.phaseWindow?.measurementEnd && run.executionStartedAt) {
    timeline.push(`Measure ${formatRelativeWindow(run.executionStartedAt, run.phaseWindow.measurementEnd)}`);
  }
  if (run.phaseWindow?.cooldownEnd) {
    timeline.push(`Finish ${formatDate(run.phaseWindow.cooldownEnd)}`);
  } else if (run.completedAt) {
    timeline.push(`Finish ${formatDate(run.completedAt)}`);
  }
  const progress = run.progress || {};
  const elapsed = Number(progress.elapsedSeconds || 0);
  const total = Number(progress.totalSeconds || 0);
  let remainingText = "";
  if (["warmup", "measuring", "cooldown"].includes(String(run.status || "").toLowerCase()) && total > 0) {
    remainingText = `Remaining ${formatDuration(Math.max(0, total - elapsed))}`;
  }
  return {
    headline: `${currentPhaseLabel(run)}${remainingText ? `, ${remainingText}` : ""}`,
    detail: compactList(timeline),
  };
}

function renderResourceConfig() {
  const grid = document.getElementById("resourceConfigGrid");
  if (!grid) return;
  const defaults = defaultResourceConfigForBroker();
  const rc = currentResourceConfig();
  grid.innerHTML = `
    <label><span>CPU request</span><input type="text" data-rc="cpuRequest" value="${rc.cpuRequest || defaults.cpuRequest}" /></label>
    <label><span>CPU limit</span><input type="text" data-rc="cpuLimit" value="${rc.cpuLimit || defaults.cpuLimit}" /></label>
    <label><span>Memory request</span><input type="text" data-rc="memoryRequest" value="${rc.memoryRequest || defaults.memoryRequest}" /></label>
    <label><span>Memory limit</span><input type="text" data-rc="memoryLimit" value="${rc.memoryLimit || defaults.memoryLimit}" /></label>
    <label><span>Storage</span><input type="text" data-rc="storageSize" value="${rc.storageSize || defaults.storageSize}" /></label>
    <label><span>Storage class</span><input type="text" data-rc="storageClassName" value="${rc.storageClassName || defaults.storageClassName || ""}" placeholder="cluster default" /></label>
    <label class="resource-config-replica-toggle"><span>Replica override</span><label class="toggle"><input type="checkbox" data-rc-replica-toggle="true" ${rc.__replicasCustom ? "checked" : ""} /><span>${rc.__replicasCustom ? "Custom" : "Auto"}</span></label></label>
    <label><span>Broker replicas</span><input type="number" data-rc="replicas" value="${rc.replicas || defaults.replicas || 1}" min="1" ${rc.__replicasCustom ? "" : "disabled"} /></label>
    <button class="button resource-reset-btn" type="button" id="resetResourceConfigButton">Reset to recommended</button>
  `;
  const replicaToggle = grid.querySelector("[data-rc-replica-toggle]");
  if (replicaToggle) {
    replicaToggle.addEventListener("change", (event) => {
      const enabled = !!event.target.checked;
      rc.__replicasCustom = enabled;
      if (!enabled) {
        rc.replicas = defaults.replicas;
      }
      renderBrokerCards();
      renderResourceConfig();
      renderFinalRunSummary();
    });
  }
  grid.querySelectorAll("[data-rc]").forEach((input) => {
    input.addEventListener("input", (e) => {
      const key = e.target.dataset.rc;
      if (key === "replicas" && !rc.__replicasCustom) {
        return;
      }
      if (key === "replicas") {
        currentResourceConfig().__replicasCustom = true;
        currentResourceConfig()[key] = Math.max(1, Number(e.target.value || defaults.replicas || 1));
      } else {
        currentResourceConfig().__custom[key] = true;
        currentResourceConfig()[key] = e.target.value;
      }
      renderBrokerCards();
      renderFinalRunSummary();
    });
  });
  const resetBtn = document.getElementById("resetResourceConfigButton");
  if (resetBtn) {
    resetBtn.addEventListener("click", () => {
      resetResourceConfigForBroker();
      renderBrokerCards();
      renderResourceConfig();
      renderFinalRunSummary();
    });
  }
}

function renderRunList() {
  elements.runList.innerHTML = "";
  if (!state.runs.length) {
    return;
  }

  const activeStatuses = new Set(["scheduled", "preparing", "warmup", "measuring", "cooldown", "finalizing", "cleaning"]);
  const activeRuns = state.runs.filter((run) => activeStatuses.has(String(run.status || "").toLowerCase()));
  const historyRuns = state.runs.filter((run) => !activeStatuses.has(String(run.status || "").toLowerCase()));
  const sections = [
    { title: "Active", runs: activeRuns },
    { title: "History", runs: historyRuns },
  ].filter((section) => section.runs.length);

  for (const section of sections) {
    const sectionEl = document.createElement("section");
    sectionEl.className = "run-section";
    sectionEl.innerHTML = `<div class="run-section-head"><h3>${section.title}</h3><span>${section.runs.length}</span></div>`;

    for (const run of section.runs) {
    const latestEvent = latestRunEvent(run.id);
    const failureEvent = failureEventForRun(run.id);
    const displayEvent = failureEvent || latestEvent;
    const stages = stageStateForRun(run);
    const overallPercent = progressPercentForRun(run);
    const timeProgress = runTimeProgressSummary(run);

    const card = document.createElement("article");
    card.className = "run-card";
    card.innerHTML = `
      <div class="run-head">
        <div>
          <h3>${run.name}</h3>
          <div class="run-id">${run.id}</div>
        </div>
        <div class="tag-row">
          <span class="badge">${brokerLabel(run.brokerId)}</span>
          <span class="badge">${runSetupLabel(run)}</span>
          <span class="badge">${deploymentLabel(run.deploymentMode)}</span>
          <span class="badge">${run.protocol}</span>
          <span class="badge ${statusClass(run.status)}">${String(run.status).toUpperCase()}</span>
        </div>
      </div>

      <div class="run-meta">
        <div>
          <span>Broker</span>
          <strong>${brokerLabel(run.brokerId)}</strong>
          <p class="hint">${runSetupLabel(run)}</p>
        </div>
        <div>
          <span>Traffic</span>
          <strong>${runLoadProfileLabel(run)}</strong>
          <p class="hint">${formatBytesCompact(run.messageSizeBytes)} payload</p>
        </div>
        <div>
          <span>Scale</span>
          <strong>${run.producers} producer${run.producers > 1 ? "s" : ""}, ${run.consumers} consumer${run.consumers > 1 ? "s" : ""}</strong>
        </div>
        <div>
          <span>Test</span>
          <strong>Measured ${formatDuration(run.measurementSeconds || 0)}</strong>
          <p class="hint">${compactList([`Warmup ${formatDuration(run.warmupSeconds || 0)}`, `Cooldown ${formatDuration(run.cooldownSeconds || 0)}`])}</p>
        </div>
        <div>
          <span>Timeline</span>
          <strong>${timeProgress.headline}</strong>
          ${timeProgress.detail ? `<p class="hint">${timeProgress.detail}</p>` : ""}
        </div>
        <div>
          <span>Resources</span>
          <strong>${resourceConfigSummary(run.resourceConfig || {})}</strong>
        </div>
      </div>

      <div class="run-progress">
        <div class="progress-row"><span>Progress</span><strong>${overallPercent.toFixed(0)}%</strong></div>
        <div class="progress-track"><span class="progress-fill run" style="width:${overallPercent}%"></span></div>
        <div class="stage-row">
          ${stages
            .map(
              (stage) => `<span class="stage-pill ${stage.done ? "done" : stage.active ? "active" : "pending"}">${stage.label}</span>`
            )
            .join("")}
        </div>
        <p class="hint stage-message">${displayEvent ? friendlyRunMessage(displayEvent.message) : "Waiting to start."}</p>
      </div>

      <div class="run-meta">
        <div>
          <span>Topology</span>
          <strong>${run.topologyDeletedAt ? "Removed" : run.runtimeNamespace || "-"}</strong>
          <p class="hint">${run.topologyReadyAt ? `Ready ${formatDate(run.topologyReadyAt)}` : "Provisioning tracked in the event stream."}</p>
        </div>
      </div>

      <div class="run-actions">
        <button class="button" data-action="reuse" data-id="${run.id}">Reuse</button>
        <button class="button" data-action="stop" data-id="${run.id}" ${["completed", "stopped", "cleaning"].includes(run.status) ? "disabled" : ""}>Stop</button>
        <button class="button danger" data-action="delete" data-id="${run.id}" ${["scheduled", "preparing", "warmup", "measuring", "cooldown", "finalizing", "cleaning"].includes(run.status) ? "disabled" : ""}>Clear</button>
      </div>
    `;
      sectionEl.appendChild(card);
    }

    elements.runList.appendChild(sectionEl);
  }
}

function inferSetupPresetFromRun(run) {
  const requested = String(run?.brokerTuning?.setupPreset || "").trim();
  if (requested && SETUP_PRESETS[requested]) {
    return requested;
  }
  return String(run?.configMode || "").trim() === "baseline" ? "defaults" : "latency";
}

function suggestReuseRunName(run) {
  const base = String(run?.name || "benchmark").trim() || "benchmark";
  const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z");
  return `${base}-${stamp}`;
}

function applyResourceConfigDraftFromRun(run) {
  const brokerId = String(run?.brokerId || state.selectedBrokerId || "kafka").trim() || "kafka";
  const defaults = defaultResourceConfigForBroker(brokerId);
  const recommended = recommendedResourceConfigForBroker(brokerId, defaults);
  const source = { ...(run?.resourceConfig || {}) };
  const custom = {};
  for (const key of RESOURCE_CONFIG_KEYS) {
    const value = source[key];
    if (value !== undefined && value !== null && String(value).trim() !== "") {
      custom[key] = String(value);
    }
  }
  const previous = {
    ...source,
    __recommended: recommended,
    __custom: custom,
    __replicasCustom: Object.prototype.hasOwnProperty.call(source, "replicas"),
  };
  state.resourceConfigByBroker[brokerId] = buildResourceConfigDraft(defaults, previous);
}

function loadRunIntoBenchmark(run) {
  if (!run) {
    return;
  }
  state.selectedBrokerId = String(run.brokerId || "kafka").trim() || "kafka";
  elements.brokerIdInput.value = state.selectedBrokerId;
  renderBrokerCards();
  renderProtocolOptions();

  const setupPreset = inferSetupPresetFromRun(run);
  elements.configModeSelect.value = setupPreset;
  elements.deploymentModeSelect.value = String(run.deploymentMode || "normal").trim() || "normal";
  state.protocolByBroker[state.selectedBrokerId] = String(run.protocol || brokerMeta()?.defaultProtocol || "").trim() || brokerMeta()?.defaultProtocol || "";
  renderProtocolOptions();
  if (state.protocolByBroker[state.selectedBrokerId]) {
    elements.protocolSelect.value = state.protocolByBroker[state.selectedBrokerId];
  }

  const scenarioExists = visiblePresets().some((preset) => preset.id === run.scenarioId);
  elements.scenarioSelect.value = scenarioExists ? run.scenarioId : "";
  if (scenarioExists) {
    applySetup(run.scenarioId);
  } else {
    applyModePreset(setupPreset);
    resetResourceConfigForBroker(state.selectedBrokerId);
  }

  elements.runNameInput.value = suggestReuseRunName(run);
  elements.startsAtInput.value = "";
  elements.messageRateInput.value = String(run.messageRate || 1);
  elements.messageSizeBytesInput.value = String(run.messageSizeBytes || 1024);
  elements.producersInput.value = String(run.producers || 1);
  elements.consumersInput.value = String(run.consumers || 1);
  elements.warmupSecondsInput.value = String(run.warmupSeconds || 0);
  elements.measurementSecondsInput.value = String(run.measurementSeconds || 60);
  elements.cooldownSecondsInput.value = String(run.cooldownSeconds || 0);

  const transportOptions = run.transportOptions || {};
  elements.rateProfileSelect.value = String(transportOptions.rateProfileKind || "constant").trim() || "constant";
  const mergedTuningModel = buildTuningModel(setupPreset);
  const savedTuning = run.brokerTuning || {};
  mergedTuningModel.setupPreset = setupPreset;
  mergedTuningModel.profileMode = run.configMode || mergedTuningModel.profileMode;
  for (const scope of ["broker", "producer", "consumer"]) {
    mergedTuningModel[scope] = {
      ...(mergedTuningModel[scope] || {}),
      ...((savedTuning[scope] && typeof savedTuning[scope] === "object") ? savedTuning[scope] : {}),
    };
  }
  state.tuningPresetKind = setupPresetMeta(setupPreset).tuningPreset;
  state.tuningModel = {
    ...mergedTuningModel,
    profileId: savedTuning.profileId ?? mergedTuningModel.profileId,
    setupPreset,
    tuningPreset: savedTuning.tuningPreset ?? mergedTuningModel.tuningPreset,
  };
  syncTuningHiddenInput();

  applyResourceConfigDraftFromRun(run);
  renderConfigEditor();
  renderBrokerCards();
  renderResourceConfig();
  updateWizardState();
  setActiveTab("benchmark");
  setWizardStep(3);
  showFlash(`Loaded ${run.name} into Benchmark.`, "success");
}

function measuredRuns() {
  return state.runs.filter(
    (run) =>
      run.status === "completed" &&
      run.metrics?.source === "benchmark-agent" &&
      Number(run.metrics?.summary?.endToEndLatencyMs?.count || 0) > 0
  );
}

function normalizeSelections(
  idSet,
  availableRuns = state.runs,
  maxCount = Number.POSITIVE_INFINITY,
  ensureOne = true
) {
  const availableIds = new Set(availableRuns.map((run) => run.id));
  for (const id of Array.from(idSet)) {
    if (!availableIds.has(id)) idSet.delete(id);
  }
  const normalized = Array.from(idSet).slice(0, maxCount);
  idSet.clear();
  normalized.forEach((id) => idSet.add(id));
  if (ensureOne && !idSet.size && availableRuns.length) {
    idSet.add(availableRuns[0].id);
  }
}

function selectedVisualizationRuns(availableRuns = measuredRuns()) {
  return availableRuns.filter((run) => state.vizSelectedIds.has(run.id));
}

function ensureVisualizationSelection(availableRuns = measuredRuns()) {
  normalizeSelections(state.vizSelectedIds, availableRuns, Number.POSITIVE_INFINITY, false);
  if (!availableRuns.length) {
    state.vizSelectedIds.clear();
    return;
  }
  if (!state.vizSelectedIds.size && !state.vizSelectionTouched) {
    state.vizSelectedIds.add(availableRuns[0].id);
  }
}

function selectedReportRuns(availableRuns = measuredRuns()) {
  return availableRuns.filter((run) => state.reportSelectedIds.has(run.id));
}

async function exportResults(selection = "selected") {
  const availableRuns = measuredRuns();
  const runs = selection === "all" ? availableRuns : selectedVisualizationRuns(availableRuns);
  if (!runs.length) {
    showFlash("Select at least one completed measured run to export.", "warning");
    return;
  }
  const { blob, fileName } = await fetchDownload("/api/runs/export", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ runIds: runs.map((run) => run.id) }),
  }, "concerto-bus-results.json");
  triggerBrowserDownload(blob, fileName);
  showFlash(`Exported ${runs.length} run${runs.length === 1 ? "" : "s"}.`, "success");
}

async function importResultsFile(file) {
  if (!file) {
    return;
  }
  let payload;
  try {
    payload = safeJsonParse(await file.text(), null);
  } catch {
    payload = null;
  }
  if (!payload || typeof payload !== "object") {
    showFlash("Select a valid results export file.", "warning");
    return;
  }
  const result = await fetchJson("/api/runs/import", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  await refreshData();
  const importedRuns = Array.isArray(result.importedRuns) ? result.importedRuns : [];
  const importedIds = importedRuns.map((run) => run.id).filter(Boolean);
  if (importedIds.length) {
    state.vizSelectionTouched = true;
    state.reportSelectionTouched = true;
    state.vizSelectedIds.clear();
    state.reportSelectedIds.clear();
    importedIds.forEach((id) => {
      state.vizSelectedIds.add(id);
      state.reportSelectedIds.add(id);
    });
  }
  setActiveTab("results");
  showFlash(`Imported ${Number(result.count || importedIds.length || 0)} run${Number(result.count || importedIds.length || 0) === 1 ? "" : "s"}.`, "success");
}

function ensureReportSelection(availableRuns = measuredRuns()) {
  normalizeSelections(state.reportSelectedIds, availableRuns, Number.POSITIVE_INFINITY, false);
  if (!availableRuns.length) {
    state.reportSelectedIds.clear();
    return;
  }
  if (!state.reportSelectedIds.size && !state.reportSelectionTouched) {
    state.reportSelectedIds.add(availableRuns[0].id);
  }
}

function renderReportRunPicker(availableRuns = measuredRuns()) {
  if (!availableRuns.length) {
    elements.reportRunPicker.innerHTML = `<p class="empty">No completed measured runs yet.</p>`;
    return;
  }

  const runs = selectedReportRuns(availableRuns);
  const selectedCount = runs.length;
  const selectionMode = selectedCount > 1 ? "Included runs" : selectedCount === 1 ? "Included run" : "Included runs";
  const selectionHint = selectedCount
    ? `${selectedCount} selected. The PDF will use only these runs.`
    : "Choose the runs that belong in the report.";

  elements.reportRunPicker.innerHTML = `
    <div class="viz-picker-head">
      <div>
        <strong>${selectionMode}</strong>
        <p>${selectionHint}</p>
      </div>
      <div class="viz-picker-actions">
        <button class="button" type="button" data-report-action="latest">Latest run</button>
        <button class="button" type="button" data-report-action="latest-two">Latest 2</button>
        <button class="button" type="button" data-report-action="all">All</button>
        <button class="button" type="button" data-report-action="clear">Clear</button>
      </div>
    </div>
    <div class="viz-run-grid">
      ${availableRuns
        .map((run) => {
          const selected = state.reportSelectedIds.has(run.id);
          return `
            <button type="button" class="viz-run-option ${selected ? "selected" : ""}" data-report-run-id="${run.id}" title="${run.name}">
              <span>${run.name}</span>
              <span class="viz-run-check" aria-hidden="true">${selected ? "&#10003;" : ""}</span>
            </button>
          `;
        })
        .join("")}
    </div>
  `;

  elements.reportRunPicker.querySelectorAll("[data-report-run-id]").forEach((node) => {
    node.addEventListener("click", () => {
      const runId = node.dataset.reportRunId;
      if (!runId) return;
      state.reportSelectionTouched = true;
      if (state.reportSelectedIds.has(runId)) {
        state.reportSelectedIds.delete(runId);
      } else {
        state.reportSelectedIds.add(runId);
      }
      renderReportPreview();
    });
  });

  elements.reportRunPicker.querySelectorAll("[data-report-action]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      state.reportSelectionTouched = true;
      const action = button.dataset.reportAction;
      if (action === "latest") {
        state.reportSelectedIds.clear();
        if (availableRuns[0]) state.reportSelectedIds.add(availableRuns[0].id);
      }
      if (action === "latest-two") {
        state.reportSelectedIds.clear();
        availableRuns.slice(0, 2).forEach((run) => state.reportSelectedIds.add(run.id));
      }
      if (action === "all") {
        state.reportSelectedIds.clear();
        availableRuns.forEach((run) => state.reportSelectedIds.add(run.id));
      }
      if (action === "clear") {
        state.reportSelectedIds.clear();
      }
      renderReportPreview();
    });
  });
}

function renderVisualizationPicker(availableRuns = measuredRuns()) {
  if (!availableRuns.length) {
    elements.vizRunPicker.innerHTML = `<p class="empty">No completed measured runs yet.</p>`;
    return;
  }

  const selectedRuns = selectedVisualizationRuns(availableRuns);
  const selectedCount = selectedRuns.length;
  const selectionMode = selectedCount > 1 ? "Selected runs" : "Selected run";
  const selectionHint = selectedCount
    ? `${selectedCount} selected. One run shows detail. Several runs switch to comparison.`
    : "Choose one run for detail or select several runs to compare them.";

  elements.vizRunPicker.innerHTML = `
    <div class="viz-picker-head">
      <div>
        <strong>${selectionMode}</strong>
        <p>${selectionHint}</p>
      </div>
      <div class="viz-picker-actions">
        <button class="button" type="button" data-viz-action="latest">Latest run</button>
        <button class="button" type="button" data-viz-action="all">Compare all</button>
        <button class="button" type="button" data-viz-action="clear">Clear</button>
      </div>
    </div>
    <div class="viz-run-grid">
      ${availableRuns
        .map((run) => {
          const selected = state.vizSelectedIds.has(run.id);
          return `
            <button type="button" class="viz-run-option ${selected ? "selected" : ""}" data-run-id="${run.id}" title="${run.name}">
              <span>${run.name}</span>
                <span class="viz-run-check" aria-hidden="true">${selected ? "&#10003;" : ""}</span>
            </button>
          `;
        })
        .join("")}
    </div>
  `;

  elements.vizRunPicker.querySelectorAll("[data-run-id]").forEach((node) => {
    node.addEventListener("click", (event) => {
      const runId = node.dataset.runId;
      if (!runId) return;
      const clearRequested = event.target instanceof HTMLElement && event.target.closest("[data-viz-action]");
      if (clearRequested) return;
      state.vizSelectionTouched = true;
      if (state.vizSelectedIds.has(runId)) {
        state.vizSelectedIds.delete(runId);
      } else {
        state.vizSelectedIds.add(runId);
      }
      renderVisualizations();
    });
  });

  elements.vizRunPicker.querySelectorAll("[data-viz-action]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      state.vizSelectionTouched = true;
      const action = button.dataset.vizAction;
      if (action === "latest") {
        state.vizSelectedIds.clear();
        if (availableRuns[0]) state.vizSelectedIds.add(availableRuns[0].id);
      }
      if (action === "all") {
        state.vizSelectedIds.clear();
        availableRuns.forEach((run) => state.vizSelectedIds.add(run.id));
      }
      if (action === "clear") {
        state.vizSelectedIds.clear();
      }
      renderVisualizations();
    });
  });
}

function prepareCanvas() {}
function drawChartFrame() {}
function drawValueGrid() {}
function drawLegend() {}
function drawChartHints() {}

function drawEmptyState(canvas, message = "No data") {
  const id = canvas.id || canvas.getAttribute("id");
  if (chartInstances[id]) { chartInstances[id].destroy(); delete chartInstances[id]; }
  const ctx = canvas.getContext("2d");
  const w = canvas.clientWidth || 300;
  const h = canvas.clientHeight || 220;
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = "#87abc8";
  ctx.font = "13px 'Plus Jakarta Sans'";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText(message, w / 2, h / 2);
}

function drawGroupedBarChart(canvas, labels, series, progress = 1, options = {}) {
  if (!labels.length || !series.length) { drawEmptyState(canvas); return; }
  const id = canvas.id;
  const horizontal = options.indexAxis === "y";
  const datasets = series.map((item) => ({
    label: item.name,
    data: item.values,
    backgroundColor: item.color + "b3",
    borderColor: item.color,
    borderWidth: 1,
    borderRadius: 8,
    barPercentage: 0.7,
    categoryPercentage: 0.8,
  }));
  getOrCreateChart(id, {
    type: "bar",
    data: { labels, datasets },
    options: {
      indexAxis: horizontal ? "y" : "x",
      responsive: true,
      maintainAspectRatio: false,
      layout: { padding: { top: 4, right: 10, bottom: 2, left: 2 } },
      animation: progress < 1 ? { duration: 0 } : undefined,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: {
          position: "top",
          align: "start",
          labels: {
            color: "#d2e7f8",
            font: { family: "'Plus Jakarta Sans'", size: 11, weight: "600" },
            boxWidth: 10,
            boxHeight: 10,
            usePointStyle: true,
            pointStyle: "rectRounded",
            padding: 14,
          },
        },
        tooltip: {
          backgroundColor: "rgba(7,21,34,0.95)", titleColor: "#eaf6ff", bodyColor: "#accbe6",
          borderColor: "rgba(148,185,225,0.3)", borderWidth: 1, padding: 10, cornerRadius: 8,
          bodyFont: { family: "'IBM Plex Mono'", size: 11 },
          callbacks: {
            label: (ctx) => {
              const parsedValue = horizontal ? ctx.parsed.x : ctx.parsed.y;
              return `${ctx.dataset.label}: ${options.labelFormatter ? options.labelFormatter(parsedValue) : formatMetric(parsedValue, 2)}`;
            },
          },
        },
      },
      scales: {
        x: {
          grid: { color: "rgba(148,185,225,0.08)", drawTicks: false },
          border: { display: false },
          ticks: {
            color: "#90b1cb",
            font: { family: "'Plus Jakarta Sans'", size: 10 },
            maxRotation: horizontal ? 0 : 32,
            minRotation: 0,
            autoSkip: true,
            maxTicksLimit: horizontal ? 8 : 10,
            padding: 8,
          },
          title: options.xLabel ? { display: true, text: options.xLabel, color: "#8fb3cf", font: { size: 10 } } : undefined,
          beginAtZero: true,
        },
        y: {
          grid: { color: horizontal ? "rgba(148,185,225,0.08)" : "rgba(148,185,225,0.06)", drawTicks: false },
          border: { display: false },
          ticks: {
            color: horizontal ? "#d8ebfb" : "#8fb3cf",
            font: { family: horizontal ? "'Plus Jakarta Sans'" : "'IBM Plex Mono'", size: 10, weight: horizontal ? "600" : "500" },
            padding: 8,
          },
          title: options.yLabel ? { display: true, text: options.yLabel, color: "#8fb3cf", font: { size: 10 } } : undefined,
          beginAtZero: true,
        },
      },
    },
  });
}

function drawLineChart(canvas, labels, series, progress = 1, options = {}) {
  if (!labels.length || !series.length) { drawEmptyState(canvas); return; }
  const id = canvas.id;
  const visibleLen = Math.max(2, Math.ceil(labels.length * progress));
  const datasets = series.map((item) => ({
    label: item.name,
    data: (item.values || []).slice(0, visibleLen),
    borderColor: item.color,
    backgroundColor: item.color + "12",
    borderWidth: 2.2,
    pointRadius: 0,
    pointHoverRadius: 4,
    pointHoverBorderWidth: 2,
    pointHoverBackgroundColor: item.color,
    cubicInterpolationMode: "monotone",
    tension: 0.28,
    spanGaps: true,
    fill: item.fill || false,
    borderDash: item.borderDash || [],
  }));
  getOrCreateChart(id, {
    type: "line",
    data: { labels: labels.slice(0, visibleLen), datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      layout: { padding: { top: 4, right: 10, bottom: 2, left: 2 } },
      animation: progress < 1 ? { duration: 0 } : undefined,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: {
          position: "top",
          align: "start",
          labels: {
            color: "#d2e7f8",
            font: { family: "'Plus Jakarta Sans'", size: 11, weight: "600" },
            boxWidth: 12,
            usePointStyle: true,
            pointStyle: "line",
            padding: 14,
          },
        },
        tooltip: {
          backgroundColor: "rgba(7,21,34,0.95)", titleColor: "#eaf6ff", bodyColor: "#accbe6",
          borderColor: "rgba(148,185,225,0.3)", borderWidth: 1, padding: 10, cornerRadius: 8,
          titleFont: { family: "'Plus Jakarta Sans'", size: 12 },
          bodyFont: { family: "'IBM Plex Mono'", size: 11 },
          callbacks: { label: (ctx) => `${ctx.dataset.label}: ${options.valueFormatter ? options.valueFormatter(ctx.parsed.y) : formatMetric(ctx.parsed.y, 2)}${options.yLabel ? " " + options.yLabel : ""}` },
        },
      },
      scales: {
        x: {
          grid: { color: "rgba(148,185,225,0.08)", drawTicks: false },
          border: { display: false },
          ticks: { color: "#90b1cb", font: { family: "'Plus Jakarta Sans'", size: 10 }, maxTicksLimit: 8, autoSkip: true, padding: 8 },
          title: options.xLabel ? { display: true, text: options.xLabel, color: "#8fb3cf", font: { size: 10 } } : undefined,
        },
        y: {
          grid: { color: "rgba(148,185,225,0.08)", drawTicks: false },
          border: { display: false },
          ticks: { color: "#8fb3cf", font: { family: "'IBM Plex Mono'", size: 10 }, padding: 8 },
          title: options.yLabel ? { display: true, text: options.yLabel, color: "#8fb3cf", font: { size: 10 } } : undefined,
          beginAtZero: true,
        },
      },
    },
  });
}

function drawHistogramChart(canvas, histogram, progress = 1, options = {}) {
  if (!histogram.length) { drawEmptyState(canvas, "No latency distribution"); return; }
  const labels = histogram.map((b) => {
    const upper = b.upperMs;
    if (upper === null || upper === undefined || !Number.isFinite(Number(upper))) return `>${b.lowerMs}`;
    return `${b.lowerMs}-${upper}`;
  });
  drawGroupedBarChart(canvas, labels,
    [{ name: "messages", color: palette[4], values: histogram.map((b) => Number(b.count || 0)) }],
    progress,
    { xLabel: options.xLabel || "latency bucket (ms)", yLabel: options.yLabel || "messages", labelFormatter: (v) => formatNumber(Math.round(v)) }
  );
}

function setVisualizationCard(kind, title, caption, tags = []) {
  const map = {
    latency: [elements.latencyCardTitle, elements.latencyCardCaption, elements.latencyCardMeta],
    percentile: [elements.percentileCardTitle, elements.percentileCardCaption, elements.percentileCardMeta],
    distribution: [elements.distributionCardTitle, elements.distributionCardCaption, elements.distributionCardMeta],
    spread: [elements.spreadCardTitle, elements.spreadCardCaption, elements.spreadCardMeta],
    throughput: [elements.throughputCardTitle, elements.throughputCardCaption, elements.throughputCardMeta],
    resource: [elements.resourceCardTitle, elements.resourceCardCaption, elements.resourceCardMeta],
  };
  const [titleEl, captionEl, metaEl] = map[kind];
  titleEl.textContent = title;
  captionEl.textContent = caption || "";
  captionEl.hidden = !caption;
  metaEl.innerHTML = "";
  metaEl.hidden = true;
}

function renderVizSummary(runs) {
  if (!runs.length) {
    elements.vizSummary.innerHTML = `<p class="empty">Select one run for detail or several runs for comparison.</p>`;
    return;
  }
  if (runs.length === 1) {
    const primary = runs[0];
    const summary = primary.metrics?.summary || {};
    const latency = summary.endToEndLatencyMs || {};
    const throughput = summary.throughput || {};
    elements.vizSummary.innerHTML = `
      <article class="metric-card">
        <span class="metric-label">Selected run</span>
        <strong>${primary.name}</strong>
        <p class="hint">${compactList([brokerLabel(primary.brokerId), runSetupLabel(primary), deploymentLabel(primary.deploymentMode)])}</p>
      </article>
      <article class="metric-card">
        <span class="metric-label">p50 latency</span>
        <strong>${formatMetric(latency.p50)} ms</strong>
      </article>
      <article class="metric-card">
        <span class="metric-label">p95 latency</span>
        <strong>${formatMetric(latency.p95)} ms</strong>
      </article>
      <article class="metric-card">
        <span class="metric-label">p99 latency</span>
        <strong>${formatMetric(latency.p99)} ms</strong>
      </article>
      <article class="metric-card">
        <span class="metric-label">Delivered msg/s</span>
        <strong>${formatNumber(throughput.achieved || 0)} msg/s</strong>
        <p class="hint">${compactList([`Target ${formatNumber(primary.messageRate || 0)} msg/s`, `${formatMetric(summary.deliverySuccessRate, 1)}% success`, formatBytesPerSecond(throughput.achievedBytesPerSec || 0)])}</p>
      </article>
      <article class="metric-card">
        <span class="metric-label">Duration</span>
        <strong>${runDurationLabel(primary)}</strong>
        <p class="hint">${compactList([`Warmup ${formatDuration(primary.warmupSeconds || 0)}`, `Measure ${formatDuration(primary.measurementSeconds || 0)}`, `Cooldown ${formatDuration(primary.cooldownSeconds || 0)}`])}</p>
      </article>
    `;
    return;
  }

  const byP99 = [...runs].sort(
    (left, right) =>
      Number(left.metrics?.summary?.endToEndLatencyMs?.p99 || Number.MAX_SAFE_INTEGER) -
      Number(right.metrics?.summary?.endToEndLatencyMs?.p99 || Number.MAX_SAFE_INTEGER)
  );
  const byP50 = [...runs].sort(
    (left, right) =>
      Number(left.metrics?.summary?.endToEndLatencyMs?.p50 || Number.MAX_SAFE_INTEGER) -
      Number(right.metrics?.summary?.endToEndLatencyMs?.p50 || Number.MAX_SAFE_INTEGER)
  );
  const byThroughput = [...runs].sort(
    (left, right) =>
      Number(right.metrics?.summary?.throughput?.achieved || 0) -
      Number(left.metrics?.summary?.throughput?.achieved || 0)
  );
  const bySuccess = [...runs].sort(
    (left, right) =>
      Number(right.metrics?.summary?.deliverySuccessRate || 0) -
      Number(left.metrics?.summary?.deliverySuccessRate || 0)
  );

  elements.vizSummary.innerHTML = `
    <article class="metric-card">
      <span class="metric-label">Selected runs</span>
      <strong>${runs.length}</strong>
      <p class="hint">${summarizeSelectedRunNames(runs)}</p>
    </article>
    <article class="metric-card">
      <span class="metric-label">Lowest p99</span>
      <strong>${formatMetric(byP99[0]?.metrics?.summary?.endToEndLatencyMs?.p99)} ms</strong>
      <p class="hint">${byP99[0]?.name || "-"}</p>
    </article>
    <article class="metric-card">
      <span class="metric-label">Lowest p50</span>
      <strong>${formatMetric(byP50[0]?.metrics?.summary?.endToEndLatencyMs?.p50)} ms</strong>
      <p class="hint">${byP50[0]?.name || "-"}</p>
    </article>
    <article class="metric-card">
      <span class="metric-label">Highest throughput</span>
      <strong>${formatNumber(byThroughput[0]?.metrics?.summary?.throughput?.achieved || 0)} msg/s</strong>
      <p class="hint">${byThroughput[0]?.name || "-"}</p>
    </article>
    <article class="metric-card">
      <span class="metric-label">Best success rate</span>
      <strong>${formatMetric(bySuccess[0]?.metrics?.summary?.deliverySuccessRate, 1)}%</strong>
      <p class="hint">${bySuccess[0]?.name || "-"}</p>
    </article>
    <article class="metric-card">
      <span class="metric-label">Duration</span>
      <strong>${summarizeRunDurations(runs)}</strong>
      <p class="hint">${runs.length === 1 ? runs[0].name : `${runs.length} selected runs`}</p>
    </article>
  `;
}

function drawVisualFrame(runs, progress = 1) {
  if (!runs.length) {
    elements.resourceVizCard.hidden = true;
    setVisualizationCard("latency", "End-to-end latency", "");
    setVisualizationCard("percentile", "Percentiles", "");
    setVisualizationCard("distribution", "Distribution", "");
    setVisualizationCard("spread", "Tail spread", "");
    setVisualizationCard("throughput", "Throughput", "");
    setVisualizationCard("resource", "Resource context", "");
    for (const canvas of [elements.latencyChart, elements.throughputChart, elements.successChart, elements.sizeImpactChart, elements.scalingChart, elements.resourceChart]) {
      drawEmptyState(canvas, "Select runs");
    }
    return;
  }

  const multiple = runs.length > 1;
  const labels = runs.map((run) => clampLabel(run.name, 18));
  const focusRun = runs[0];
  const points = focusRun.metrics?.timeseries || [];
  const bucketed = bucketTimeseries(points);
  const timeLabels = bucketed.labels;
  const bucketLabel = bucketed.bucketSeconds > 1 ? `${bucketed.bucketSeconds}-second buckets` : "per-second detail";
  const latencyFormatter = (value) => `${Math.round(Number(value || 0))}`;
  const throughputFormatter = (value) => formatNumber(Math.round(Number(value || 0)));

  if (!multiple) {
    const focusLatency = focusRun.metrics?.summary?.endToEndLatencyMs || {};
    setVisualizationCard(
      "latency",
      "Latency over time",
      `Stabilized end-to-end latency using ${bucketLabel}.`
    );
    const latencySeries = [
      { name: "p50", color: palette[0], values: bucketed.latencyP50, fill: true },
      { name: "p95", color: palette[1], values: bucketed.latencyP95, borderDash: [6, 6] },
      { name: "p99", color: palette[2], values: bucketed.latencyP99 },
    ];
    drawLineChart(elements.latencyChart, timeLabels, latencySeries, progress, {
      valueFormatter: latencyFormatter,
      xLabel: "measured time",
      yLabel: "ms",
    });

    setVisualizationCard(
      "percentile",
      "Latency summary",
      "Median, tail, and worst observed latency for the selected run."
    );
    drawGroupedBarChart(
      elements.sizeImpactChart,
      ["p50", "p95", "p99", "max"],
      [{ name: "latency", color: palette[2], values: [focusLatency.p50, focusLatency.p95, focusLatency.p99, focusLatency.max] }],
      progress,
      {
        valueFormatter: latencyFormatter,
        labelFormatter: (value) => `${formatMetric(value, 0)}ms`,
        xLabel: "percentile",
        yLabel: "ms",
      }
    );

    setVisualizationCard(
      "distribution",
      "Latency distribution",
      "Message counts across latency buckets for the stabilized window."
    );
    drawHistogramChart(elements.successChart, focusRun.metrics?.histogramMs || focusRun.metrics?.histogram || [], progress, {
      xLabel: "latency bucket",
      yLabel: "messages",
    });

    setVisualizationCard(
      "spread",
      "Tail pressure",
      `How far the tail moved away from the median across ${bucketLabel}.`
    );
    const spreadSeries = [
      {
        name: "p95 - p50",
        color: palette[1],
        values: bucketed.latencyP95.map((value, index) => Math.max(0, Number(value || 0) - Number(bucketed.latencyP50[index] || 0))),
      },
      {
        name: "p99 - p50",
        color: palette[3],
        values: bucketed.latencyP99.map((value, index) => Math.max(0, Number(value || 0) - Number(bucketed.latencyP50[index] || 0))),
      },
    ];
    drawLineChart(elements.scalingChart, timeLabels, spreadSeries, progress, {
      valueFormatter: latencyFormatter,
      xLabel: "measured time",
      yLabel: "ms above p50",
    });

    setVisualizationCard(
      "throughput",
      "Delivery versus target",
      `Delivered rate compared with the intended rate using ${bucketLabel}.`
    );
    drawLineChart(elements.throughputChart, timeLabels, [
      { name: "target", color: palette[0], values: bucketed.targetRate, borderDash: [6, 6] },
      { name: "delivered", color: palette[2], values: bucketed.throughput, fill: true },
    ], progress, {
      valueFormatter: throughputFormatter,
      xLabel: "measured time",
      yLabel: "msg/s",
    });
  } else {
    setVisualizationCard(
      "latency",
      "P99 latency by run",
      "Lower bars are better."
    );
    drawGroupedBarChart(elements.latencyChart, labels, [
      { name: "p99", color: palette[2], values: runs.map((run) => run.metrics?.summary?.endToEndLatencyMs?.p99 || 0) },
    ], progress, {
      indexAxis: "y",
      valueFormatter: latencyFormatter,
      labelFormatter: (value) => `${formatMetric(value, 0)}ms`,
      xLabel: "p99 latency ms",
      yLabel: "selected run",
    });

    setVisualizationCard(
      "percentile",
      "Latency profile by run",
      "Median, p95, and p99 for each selected run."
    );
    drawGroupedBarChart(elements.sizeImpactChart, labels, [
      { name: "p50", color: palette[0], values: runs.map((run) => run.metrics?.summary?.endToEndLatencyMs?.p50 || 0) },
      { name: "p95", color: palette[1], values: runs.map((run) => run.metrics?.summary?.endToEndLatencyMs?.p95 || 0) },
      { name: "p99", color: palette[2], values: runs.map((run) => run.metrics?.summary?.endToEndLatencyMs?.p99 || 0) },
    ], progress, {
      indexAxis: "y",
      valueFormatter: latencyFormatter,
      labelFormatter: (value) => `${formatMetric(value, 0)}ms`,
      xLabel: "latency ms",
      yLabel: "selected run",
    });

    setVisualizationCard(
      "distribution",
      "Success rate by run",
      "Delivered messages divided by attempted messages."
    );
    drawGroupedBarChart(elements.successChart, labels, [
      {
        name: "success",
        color: palette[1],
        values: runs.map((run) => Number(run.metrics?.summary?.deliverySuccessRate || 0)),
      },
    ], progress, {
      indexAxis: "y",
      valueFormatter: (value) => formatMetric(value, 1),
      labelFormatter: (value) => `${formatMetric(value, 1)}%`,
      xLabel: "success %",
      yLabel: "selected run",
    });

    setVisualizationCard(
      "spread",
      "Tail pressure by run",
      "Gap between median latency and p99."
    );
    drawGroupedBarChart(elements.scalingChart, labels, [
      {
        name: "p95 - p50",
        color: palette[1],
        values: runs.map((run) =>
          Math.max(
            0,
            Number(run.metrics?.summary?.endToEndLatencyMs?.p95 || 0) -
            Number(run.metrics?.summary?.endToEndLatencyMs?.p50 || 0)
          )
        ),
      },
      {
        name: "p99 - p50",
        color: palette[3],
        values: runs.map((run) =>
          Math.max(
            0,
            Number(run.metrics?.summary?.endToEndLatencyMs?.p99 || 0) -
            Number(run.metrics?.summary?.endToEndLatencyMs?.p50 || 0)
          )
        ),
      },
    ], progress, {
      indexAxis: "y",
      valueFormatter: latencyFormatter,
      labelFormatter: (value) => `${formatMetric(value, 0)}ms`,
      xLabel: "latency gap ms",
      yLabel: "selected run",
    });

    setVisualizationCard(
      "throughput",
      "Delivery by run",
      "Target versus delivered message rate."
    );
    drawGroupedBarChart(elements.throughputChart, labels, [
      { name: "target", color: palette[0], values: runs.map((run) => run.metrics?.summary?.throughput?.target || run.messageRate || 0) },
      { name: "achieved", color: palette[2], values: runs.map((run) => run.metrics?.summary?.throughput?.achieved || 0) },
    ], progress, {
      indexAxis: "y",
      valueFormatter: throughputFormatter,
      labelFormatter: (value) => formatMetric(value, 0),
      xLabel: "msg/s",
      yLabel: "selected run",
    });
  }

  const hasResourceSamples = runs.some((run) =>
    (run.metrics?.timeseries || []).some(
      (point) => point.cpuCores !== undefined || point.memoryMB !== undefined
    )
  );
  if (!hasResourceSamples) {
    elements.resourceVizCard.hidden = true;
    drawEmptyState(elements.resourceChart, "Not collected");
    return;
  }
  elements.resourceVizCard.hidden = false;

  const resourceSeries = runs.length === 1
    ? [
        {
          name: "cpu",
          color: palette[2],
          values: bucketed.cpuCores,
        },
        {
          name: "mem GB",
          color: palette[4],
          values: bucketed.memoryGB,
        },
      ]
    : runs.map((run, index) => ({
        name: clampLabel(run.name, 18),
        color: palette[index % palette.length],
        values: (run.metrics?.timeseries || []).map((point) =>
          point.cpuCores !== undefined ? point.cpuCores : null
        ),
      }));
  if (runs.length === 1) {
    setVisualizationCard(
      "resource",
      "Resource context",
      `Broker CPU and memory alongside the run using ${bucketLabel}.`
    );
    drawLineChart(elements.resourceChart, timeLabels, resourceSeries, progress, {
      valueFormatter: (value) => Number(value || 0).toFixed(1),
      xLabel: "measured time",
      yLabel: "CPU cores or mem GB",
    });
  } else {
    setVisualizationCard(
      "resource",
      "Peak broker load",
      "Peak broker CPU and memory reached by each run."
    );
    drawGroupedBarChart(elements.resourceChart, labels, [
      {
        name: "peak broker CPU",
        color: palette[2],
        values: runs.map((run) => run.metrics?.summary?.resourceUsage?.peaks?.brokerCpuCores || 0),
      },
      {
        name: "peak broker mem GB",
        color: palette[4],
        values: runs.map((run) => Number(run.metrics?.summary?.resourceUsage?.peaks?.brokerMemoryMB || 0) / 1024),
      },
    ], progress, {
      indexAxis: "y",
      valueFormatter: (value) => formatMetric(value, 1),
      labelFormatter: (value) => formatMetric(value, 1),
      xLabel: "CPU cores or memory GB",
      yLabel: "selected run",
    });
  }
}

function visualizationSignature(runs) {
  return runs
    .map((run) => {
      const summary = run.metrics?.summary || {};
      const points = run.metrics?.timeseries || [];
      const lastPoint = points.length ? points[points.length - 1] : null;
      return [
        run.id,
        run.status,
        summary.endToEndLatencyMs?.count || 0,
        summary.endToEndLatencyMs?.p99 || 0,
        summary.throughput?.achieved || 0,
        lastPoint?.second || 0,
        lastPoint?.throughput || 0,
        lastPoint?.latencyP99Ms || lastPoint?.latencyMs || 0,
      ].join(":");
    })
    .join("|");
}

function cancelVisualizationAnimation() {
  if (state.vizAnimationHandle) {
    cancelAnimationFrame(state.vizAnimationHandle);
    state.vizAnimationHandle = 0;
  }
}

function animateVisualizationFrame(runs, signature) {
  cancelVisualizationAnimation();
  state.vizAnimationSignature = signature;
  const durationMs = 520;
  const start = performance.now();
  const step = (timestamp) => {
    if (state.vizAnimationSignature !== signature) {
      return;
    }
    const elapsed = Math.max(0, timestamp - start);
    const progress = Math.min(1, elapsed / durationMs);
    const eased = 1 - ((1 - progress) ** 3);
    drawVisualFrame(runs, Math.max(0.08, eased));
    if (progress < 1) {
      state.vizAnimationHandle = requestAnimationFrame(step);
      return;
    }
    state.vizAnimationHandle = 0;
  };
  state.vizAnimationHandle = requestAnimationFrame(step);
}

function renderVisualizations() {
  const availableRuns = measuredRuns();
  ensureVisualizationSelection(availableRuns);
  renderVisualizationPicker(availableRuns);
  const runs = selectedVisualizationRuns(availableRuns);
  renderVizSummary(runs);
  if (!runs.length) {
    cancelVisualizationAnimation();
    state.vizAnimationSignature = "";
    drawVisualFrame(runs, 1);
    return;
  }
  const signature = visualizationSignature(runs);
  if (signature === state.vizAnimationSignature) {
    if (!state.vizAnimationHandle) {
      drawVisualFrame(runs, 1);
    }
    return;
  }
  animateVisualizationFrame(runs, signature);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

function renderReportPreviewBody(runs) {
  if (!runs.length) {
    elements.reportPreview.innerHTML = `<p class="empty">Select runs to build a report.</p>`;
    return;
  }
  const title = elements.reportTitleInput.value.trim() || "Concerto Bus Benchmark Report";
  const reportMode = runs.length > 1 ? "Comparison report" : "Single-run report";
  const selectedSectionLabels = REPORT_SECTION_OPTIONS
    .filter((section) => state.reportSections.has(section.id))
    .map((section) => section.label);
  const generatedAt = new Date().toLocaleString();
  const cards = runs
    .map((run) => {
      const summary = run.metrics?.summary || {};
      const latency = summary.endToEndLatencyMs || {};
      const throughput = summary.throughput || {};
      const measured = hasMeasuredResults(run);
      const setupLabel = compactList([brokerLabel(run.brokerId), runSetupLabel(run), deploymentLabel(run.deploymentMode)]);
      const scenarioLabel = compactList([
        run.protocol.toUpperCase(),
        `${run.producers}/${run.consumers} workers`,
        formatBytesCompact(run.messageSizeBytes),
      ]);
      return `
        <article class="report-run-card">
          <div class="report-run-card-head">
            <div>
              <h4>${escapeHtml(run.name)}</h4>
              <p>${escapeHtml(setupLabel)}</p>
            </div>
            <span class="report-run-card-kicker">${escapeHtml(run.protocol.toUpperCase())}</span>
          </div>
          <div class="report-run-card-grid">
            <section class="report-data-block">
              <span class="report-data-label">Latency summary</span>
              ${
                measured
                  ? `
                <div class="report-inline-metrics" aria-label="Latency summary">
                  <span><strong>p50</strong> ${escapeHtml(`${formatMetric(latency.p50, 2)} ms`)}</span>
                  <span><strong>p95</strong> ${escapeHtml(`${formatMetric(latency.p95, 2)} ms`)}</span>
                  <span><strong>p99</strong> ${escapeHtml(`${formatMetric(latency.p99, 2)} ms`)}</span>
                </div>
                <p>Median, tail, and deep-tail latency from the stabilized measured window.</p>
              `
                  : `
                <strong>-</strong>
                <p>No measured latency available</p>
              `
              }
            </section>
            <section class="report-data-block">
              <span class="report-data-label">Delivery</span>
              <strong>${measured ? `${formatNumber(throughput.achieved || 0)} msg/s` : "-"}</strong>
              <p>${measured ? `${formatBytesPerSecond(throughput.achievedBytesPerSec || 0)}, ${formatMetric(summary.deliverySuccessRate, 2)}% success` : "No measured delivery available"}</p>
            </section>
            <section class="report-data-block">
              <span class="report-data-label">Scenario</span>
              <strong>${escapeHtml(scenarioLabel)}</strong>
              <p>${escapeHtml(runLoadProfileLabel(run))}</p>
            </section>
            <section class="report-data-block">
              <span class="report-data-label">Duration</span>
              <strong>${escapeHtml(runDurationLabel(run))}</strong>
              <p>Configured window and stabilized measured window.</p>
            </section>
            <section class="report-data-block">
              <span class="report-data-label">Resources</span>
              <strong>${escapeHtml(reportResourceSummary(run.resourceConfig || {}))}</strong>
              <p>${escapeHtml(resourceConfigSummary(run.resourceConfig || {}))}</p>
            </section>
          </div>
        </article>
      `;
    })
    .join("");
  elements.reportPreview.innerHTML = `
    <div class="report-preview-head">
      <div>
        <h3>${escapeHtml(title)}</h3>
        <p class="report-meta">${escapeHtml(reportMode)}. Generated ${escapeHtml(generatedAt)}.</p>
      </div>
      <div class="report-preview-stats">
        <span class="preview-stat"><strong>Runs</strong> ${runs.length}</span>
        <span class="preview-stat"><strong>Sections</strong> ${selectedSectionLabels.length || 0}</span>
      </div>
    </div>
    <p class="report-preview-sections">${escapeHtml(selectedSectionLabels.join(", ") || "No sections selected")}</p>
    <div class="report-run-cards">${cards}</div>
  `;
}

function renderReportSectionPicker() {
  for (const sectionId of Array.from(state.reportSections)) {
    if (!REPORT_SECTION_OPTIONS.find((section) => section.id === sectionId)) {
      state.reportSections.delete(sectionId);
    }
  }
  if (!state.reportSections.size) {
    REPORT_SECTION_OPTIONS.forEach((section) => state.reportSections.add(section.id));
  }
  elements.reportSectionPicker.innerHTML = REPORT_SECTION_OPTIONS
    .map(
      (section) => `
        <label class="report-section-chip">
          <input type="checkbox" data-report-section="${section.id}" ${state.reportSections.has(section.id) ? "checked" : ""} />
          <span>${section.label}</span>
        </label>
      `
    )
    .join("");
}

function renderReportPreview() {
  const availableRuns = measuredRuns();
  ensureReportSelection(availableRuns);
  renderReportSectionPicker();
  renderReportRunPicker(availableRuns);
  renderReportPreviewBody(selectedReportRuns(availableRuns));
}

function exportReportAsPdf() {
  const runs = selectedReportRuns(measuredRuns());
  if (!runs.length) {
    window.alert("Select at least one run in Reports.");
    return;
  }
  if (!state.reportSections.size) {
    window.alert("Select at least one report section.");
    return;
  }
  renderReportPreview();
  const title = elements.reportTitleInput.value.trim() || "Concerto Bus Benchmark Report";
  fetchJson("/api/reports", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      title,
      runIds: runs.map((run) => run.id),
      sections: Array.from(state.reportSections),
    }),
  })
    .then((report) => {
      elements.reportTitleInput.value = report.title || title;
      const link = document.createElement("a");
      link.href = report.downloadUrl;
      link.download = report.fileName || `${report.title || title}.pdf`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      showFlash(`Report ${report.title} ready.`, "success");
    })
    .catch((error) => {
      console.error(error);
      showFlash(`Report failed: ${error.message}`, "error");
    });
}

function updateHeader() {
  elements.serverTime.textContent = formatDate(state.serverTime);
  elements.runCount.textContent = String(state.runs.length);
  const active = state.runs.filter((run) =>
    ["scheduled", "preparing", "warmup", "measuring", "cooldown", "cleaning"].includes(run.status)
  ).length;
  elements.activeCount.textContent = String(active);
}

function renderOperatorStatus() {
  const operators = state.bootstrapStatus?.operators || {
    kafka: { label: "Kafka", ready: false, message: "not ready" },
    rabbitmq: { label: "RabbitMQ", ready: false, message: "not ready" },
    artemis: { label: "Artemis", ready: false, message: "not ready" },
    nats: { label: "NATS JetStream", ready: false, message: "not ready" },
  };
  elements.operatorStatus.innerHTML = Object.entries(operators)
    .map(([brokerId, operator]) => {
      const ready = !!operator.ready;
      const message = operator.message || (ready ? "ready" : "not ready");
      return `
        <span class="operator-pill ${ready ? "ready" : "not-ready"}" title="Cluster control plane ${operator.label}: ${message}">
          <span class="operator-light" aria-hidden="true"></span>
          <span>${operator.label}</span>
        </span>
      `;
    })
    .join("");
}

function updateWizardState() {
  syncTuningHiddenInput();
  updateScenarioSummary();
  updateLoadShapeSummary();
  updateScalingSummary();
  updateWindowSummary();
  renderFinalRunSummary();
  renderWizardState();
}

async function ensureStaticData() {
  if (state.staticLoaded) return;
  const [presetsPayload, brokersPayload, profilesPayload] = await Promise.all([
    fetchJson("/api/presets"),
    fetchJson("/api/brokers"),
    fetchJson("/api/broker-profiles"),
  ]);
  state.presets = presetsPayload.presets || [];
  state.rateProfiles = presetsPayload.rateProfiles || {};
  state.brokers = brokersPayload.brokers || [];
  state.brokerProfiles = profilesPayload.profiles || [];
  if (!state.brokers.find((item) => item.id === state.selectedBrokerId)) {
    state.selectedBrokerId = state.brokers[0]?.id || "kafka";
  }
  elements.brokerIdInput.value = state.selectedBrokerId;
  currentResourceConfig(state.selectedBrokerId);
  renderBrokerCards();
  renderProtocolOptions();
  renderPresetOptions();
  applyModePreset(elements.configModeSelect.value || "latency");
  updateWizardState();
  renderResourceConfig();
  state.staticLoaded = true;
}

async function refreshData() {
  await ensureStaticData();
  const [runsPayload, bootstrapPayload] = await Promise.all([
    fetchJson("/api/runs"),
    fetchJson("/api/system/bootstrap").catch(() => ({ ready: false, message: "bootstrap status unavailable" })),
  ]);
  state.runs = runsPayload.runs || [];
  state.events = runsPayload.recentEvents || [];
  state.serverTime = runsPayload.serverTime || null;
  state.bootstrapStatus = bootstrapPayload;
  const availableMeasuredRuns = measuredRuns();
  normalizeSelections(state.vizSelectedIds, availableMeasuredRuns, Number.POSITIVE_INFINITY, false);
  normalizeSelections(state.reportSelectedIds, availableMeasuredRuns, Number.POSITIVE_INFINITY, false);
  updateHeader();
  renderOperatorStatus();
  renderBrokerCards();
  renderRunList();
  if (state.activeTab === "results") {
    renderVisualizations();
  }
  if (state.activeTab === "reports") {
    renderReportPreview();
  }
  renderFinalRunSummary();
}

async function submitBenchmark(event) {
  event.preventDefault();
  if (elements.startBenchmarkButton.disabled) {
    const activeRun = blockingRun();
    showFlash(
      activeRun
        ? `Wait for ${activeRun.name} to finish cleanup before starting another run.`
        : (!brokerReadyForRun(state.selectedBrokerId)
            ? `Control plane for ${brokerLabel(state.selectedBrokerId)} is not ready yet.`
            : "Complete the required fields first."),
      "warning"
    );
    return;
  }
  syncTuningHiddenInput();
  const startsAtRaw = elements.startsAtInput.value;
  const transportOptions = {
    rateProfileKind: loadProfileValue(),
  };
  const payload = {
    name: elements.runNameInput.value.trim(),
    brokerId: state.selectedBrokerId,
    scenarioId: elements.scenarioSelect.value || null,
    configMode: effectiveConfigMode(elements.configModeSelect.value || "latency"),
    deploymentMode: elements.deploymentModeSelect.value || "normal",
    protocol: elements.protocolSelect.value,
    startsAt: startsAtRaw ? new Date(startsAtRaw).toISOString() : null,
    messageRate: Number(elements.messageRateInput.value),
    messageSizeBytes: Number(elements.messageSizeBytesInput.value),
    producers: Number(elements.producersInput.value),
    consumers: Number(elements.consumersInput.value),
    warmupSeconds: Number(elements.warmupSecondsInput.value),
    measurementSeconds: Number(elements.measurementSecondsInput.value),
    cooldownSeconds: Number(elements.cooldownSecondsInput.value),
    transportOptions,
    brokerTuning: safeJsonParse(elements.brokerTuningHidden.value, {}),
    resourceConfig: resourceConfigPayload(),
  };
  if (!payload.name) {
    showFlash("Run name is required.", "warning");
    return;
  }
  if (state.runs.some((run) => String(run.name || "").trim().toLowerCase() === payload.name.toLowerCase())) {
    showFlash(`Run name "${payload.name}" already exists.`, "warning");
    return;
  }
  const run = await fetchJson("/api/runs", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  elements.runNameInput.value = "";
  showFlash(`Run ${run.name} started.`, "success");
  await refreshData();
}

async function handleRunAction(event) {
  const button = event.target.closest("button[data-action]");
  if (!button) return;
  const runId = button.dataset.id;
  const action = button.dataset.action;
  if (action === "reuse") {
    loadRunIntoBenchmark(state.runs.find((run) => run.id === runId));
    return;
  }
  if (action === "stop") {
    await fetchJson(`/api/runs/${runId}/stop`, { method: "POST" });
    showFlash(`Run ${runId.slice(0, 8)} stopped.`, "warning");
  }
  if (action === "delete") {
    await fetchJson(`/api/runs/${runId}`, { method: "DELETE" });
    showFlash(`Namespace and run ${runId.slice(0, 8)} cleared.`, "warning");
  }
  await refreshData();
}

function bindHandlers() {
  for (const tab of elements.tabs) {
    tab.addEventListener("click", () => setActiveTab(tab.dataset.tab));
  }
  elements.stepBackButton.addEventListener("click", () => setWizardStep(state.wizardStep - 1));
  elements.stepNextButton.addEventListener("click", () => setWizardStep(state.wizardStep + 1));
  elements.stepperItems.forEach((item) => {
    item.addEventListener("click", () => {
      const step = Number(item.dataset.step);
      const unlocked = unlockedWizardStep(wizardReadiness());
      if (step > unlocked) {
        showFlash("Unlock the previous step first.", "warning");
        return;
      }
      setWizardStep(step);
    });
  });
  elements.benchmarkForm.addEventListener("submit", (event) => {
    submitBenchmark(event).catch((error) => {
      console.error(error);
      showFlash(`Start failed: ${error.message}`, "error");
    });
  });
  elements.brokerSelect.addEventListener("change", () => {
    state.selectedBrokerId = elements.brokerSelect.value;
    elements.brokerIdInput.value = elements.brokerSelect.value;
    currentResourceConfig(elements.brokerSelect.value);
    renderBrokerCards();
    renderProtocolOptions();
    applyModePreset(elements.configModeSelect.value || "latency");
    updateWizardState();
    renderResourceConfig();
  });
  elements.protocolSelect.addEventListener("change", () => {
    state.protocolByBroker[state.selectedBrokerId] = elements.protocolSelect.value;
    updateWizardState();
  });
  elements.configModeSelect.addEventListener("change", () => {
    renderProtocolOptions();
    applyModePreset(elements.configModeSelect.value || "latency");
    renderBrokerCards();
    renderResourceConfig();
    updateWizardState();
  });
  elements.deploymentModeSelect.addEventListener("change", () => {
    renderBrokerCards();
    renderResourceConfig();
    updateWizardState();
  });
  elements.scenarioSelect.addEventListener("change", (event) => applySetup(event.target.value));
  elements.rateProfileSelect.addEventListener("change", updateWizardState);
  elements.messageRateInput.addEventListener("change", updateWizardState);
  elements.configScopeTabs.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-scope]");
    if (!button) return;
    state.configScope = button.dataset.scope;
    renderConfigEditor();
  });
  elements.configEditor.addEventListener("input", updateTuningValue);
  elements.configEditor.addEventListener("change", updateTuningValue);
  for (const input of [
    elements.rateProfileSelect,
    elements.messageSizeBytesInput,
    elements.producersInput,
    elements.consumersInput,
    elements.startsAtInput,
    elements.warmupSecondsInput,
    elements.measurementSecondsInput,
    elements.cooldownSecondsInput,
    elements.runNameInput,
  ]) {
    input.addEventListener("input", updateWizardState);
    input.addEventListener("change", updateWizardState);
  }
  elements.runList.addEventListener("click", (event) => {
    handleRunAction(event).catch((error) => {
      console.error(error);
      showFlash(`Action failed: ${error.message}`, "error");
    });
  });
  elements.resetAllButton.addEventListener("click", async () => {
    try {
      await fetchJson("/api/runs/reset-all", { method: "DELETE" });
      showFlash("All run namespaces cleared.", "warning");
      await refreshData();
    } catch (error) {
      console.error(error);
      showFlash(`Clear failed: ${error.message}`, "error");
    }
  });
  elements.exportResultsButton.addEventListener("click", () => {
    exportResults("selected").catch((error) => {
      console.error(error);
      showFlash(`Export failed: ${error.message}`, "error");
    });
  });
  const resultsRefreshButton = ensureResultsRefreshButton();
  if (resultsRefreshButton) {
    resultsRefreshButton.addEventListener("click", () => {
      refreshData().catch((error) => {
        console.error(error);
        showFlash(`Refresh failed: ${error.message}`, "error");
      });
    });
  }
  elements.exportAllResultsButton.addEventListener("click", () => {
    exportResults("all").catch((error) => {
      console.error(error);
      showFlash(`Export failed: ${error.message}`, "error");
    });
  });
  elements.importResultsButton.addEventListener("click", () => {
    elements.importResultsInput.click();
  });
  elements.importResultsInput.addEventListener("change", (event) => {
    const input = event.target;
    const file = input?.files?.[0];
    importResultsFile(file).catch((error) => {
      console.error(error);
      showFlash(`Import failed: ${error.message}`, "error");
    }).finally(() => {
      elements.importResultsInput.value = "";
    });
  });
  elements.buildReportButton.addEventListener("click", renderReportPreview);
  elements.exportPdfButton.addEventListener("click", exportReportAsPdf);
  elements.reportSectionPicker.addEventListener("change", (event) => {
    const input = event.target.closest("input[data-report-section]");
    if (!input) return;
    const sectionId = input.dataset.reportSection;
    if (!sectionId) return;
    if (input.checked) {
      state.reportSections.add(sectionId);
    } else {
      if (state.reportSections.size === 1 && state.reportSections.has(sectionId)) {
        input.checked = true;
        showFlash("Keep at least one report section selected.", "warning");
        return;
      }
      state.reportSections.delete(sectionId);
    }
    renderReportPreview();
  });
}

function startPolling() {
  window.setInterval(() => {
    if (state.activeTab === "results" || state.activeTab === "reports") {
      return;
    }
    refreshData().catch((error) => {
      console.error(error);
    });
  }, 1500);
}

bindHandlers();
refreshData()
  .then(() => {
    setActiveTab("benchmark");
    startPolling();
  })
  .catch((error) => {
    console.error(error);
    showFlash(`Startup failed: ${error.message}`, "error");
  });
