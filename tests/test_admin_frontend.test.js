const test = require("node:test");
const assert = require("node:assert");

// Minimal mock DOM for Node.js test environment
class MockElement {
  constructor(tag = "div") {
    this.tagName = tag.toUpperCase();
    this.children = [];
    this.classList = {
      classes: new Set(),
      add: (c) => this.classList.classes.add(c),
      remove: (c) => this.classList.classes.delete(c),
      toggle: (c, force) => {
        if (force === undefined) {
          this.classList.classes.has(c) ? this.classList.classes.delete(c) : this.classList.classes.add(c);
        } else if (force) {
          this.classList.classes.add(c);
        } else {
          this.classList.classes.delete(c);
        }
      },
      has: (c) => this.classList.classes.has(c),
    };
    this.style = {};
    this.attributes = {};
    this.dataset = {};
    this.textContent = "";
    this._title = "";
    this._className = "";
  }
  set className(val) {
    this._className = val || "";
    this.classList.classes = new Set(this._className ? this._className.trim().split(/\s+/) : []);
  }
  get className() { return this._className; }
  set title(val) { this._title = val; }
  get title() { return this._title; }
  setAttribute(k, v) {
    this.attributes[k] = String(v);
    if (k.startsWith("data-")) {
      const camel = k.slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase());
      this.dataset[camel] = String(v);
    }
  }
  getAttribute(k) { return this.attributes[k]; }
  appendChild(child) { this.children.push(child); return child; }
  append(...nodes) {
    nodes.forEach((n) => {
      if (typeof n === "string") {
        const textNode = new MockElement("text");
        textNode.textContent = n;
        this.children.push(textNode);
      } else {
        this.children.push(n);
      }
    });
  }
  replaceChildren(...nodes) {
    this.children = [];
    this.append(...nodes);
  }
  addEventListener() {}
}

const mockDocElements = new Map();
global.document = {
  createElement: (tag) => new MockElement(tag),
  getElementById: (id) => {
    if (!mockDocElements.has(id)) {
      mockDocElements.set(id, new MockElement("div"));
    }
    return mockDocElements.get(id);
  },
  querySelectorAll: () => [],
};
global.window = {
  setTimeout: () => 1,
  clearTimeout: () => {},
  setInterval: () => 1,
  clearInterval: () => {},
};

const { formatDuration, formatRatio, renderAssetList } = require("../src/static/admin.js");

test("formatDuration renders durations correctly", () => {
  assert.strictEqual(formatDuration(45), "45s");
  assert.strictEqual(formatDuration(90), "1m 30s");
  assert.strictEqual(formatDuration(1122), "18m 42s");
  assert.strictEqual(formatDuration(4338), "1h 12m 18s");
  assert.strictEqual(formatDuration(0), "0s");
  assert.strictEqual(formatDuration(null), "—");
  assert.strictEqual(formatDuration(undefined), "—");
  assert.strictEqual(formatDuration(-5), "—");
});

test("formatRatio renders ratios with max 2 decimal places and handles edge cases", () => {
  assert.strictEqual(formatRatio(0.68), "0.68x");
  assert.strictEqual(formatRatio(1.02), "1.02x");
  assert.strictEqual(formatRatio(2.15), "2.15x");
  assert.strictEqual(formatRatio(0.6817), "0.68x");
  assert.strictEqual(formatRatio(null), "—");
  assert.strictEqual(formatRatio(undefined), "—");
  assert.strictEqual(formatRatio(""), "—");
  assert.strictEqual(formatRatio(NaN), "—");
});

test("renderAssetList renders READY asset with duration, resolution, processing time, and ratio", () => {
  const readyList = document.getElementById("readyList");
  const readyAsset = {
    vod_uuid: "3c36dfc1-61f1-4292-827e-e4fb10e2c857",
    enlace_id: "PREDI-BAYLE539",
    status: "ready",
    duration_seconds: 1650.0,
    source_width: 1920,
    source_height: 1080,
    processing_time_seconds: 1122.0,
    processing_ratio: 0.68,
    published_at: "2026-09-24T19:15:34Z",
  };

  renderAssetList([readyAsset], "readyList", "emptyReady", "readyTotal", false);

  assert.strictEqual(readyList.children.length, 1);
  const row = readyList.children[0];
  const info = row.children.find((c) => c.classList.has("asset-info"));
  assert.ok(info, "Row should have asset-info");

  const texts = info.children.map((c) => c.textContent);
  assert.ok(texts.includes("27m 30s"), "Should display duration");
  assert.ok(texts.includes("1920x1080"), "Should display resolution");
  assert.ok(texts.includes("Procesó en 18m 42s"), "Should display processing time");
  assert.ok(texts.includes("0.68x"), "Should display processing ratio");

  // Check ratio badge tooltip
  const ratioBadge = info.children.find((c) => c.classList.has("ratio-badge"));
  assert.ok(ratioBadge);
  assert.ok(ratioBadge.title.includes("Tiempo de procesamiento ÷ duración del video"));
  assert.ok(ratioBadge.title.includes("68%"));
});

test("renderAssetList renders in-progress asset showing dynamic elapsed time and dash ratio", () => {
  const processingList = document.getElementById("processingList");
  const processingAsset = {
    vod_uuid: "44444444-4444-4444-4444-444444444444",
    enlace_id: "DEMO-ACTIVE",
    status: "processing",
    progress: 40,
    duration_seconds: 1650.0,
    source_width: 1920,
    source_height: 1080,
    processing_started_at: new Date(Date.now() - 372000).toISOString(), // 6m 12s ago
    processing_finished_at: null,
    processing_time_seconds: null,
    processing_ratio: null,
  };

  renderAssetList([processingAsset], "processingList", "emptyProcessing", "processingTotal", true);

  assert.strictEqual(processingList.children.length, 1);
  const row = processingList.children[0];
  const info = row.children.find((c) => c.classList.has("asset-info"));
  assert.ok(info);

  const texts = info.children.map((c) => c.textContent);
  assert.ok(texts.some((t) => t.includes("Procesando · 40%")), "Should show status and progress");
  assert.ok(texts.some((t) => t.includes("Procesando: 6m 12s")), "Should show elapsed time");
  assert.ok(texts.includes("—"), "Should show dash for in-progress ratio");

  // Verify timer element has data-started-at attribute for live updates
  const timerElem = info.children.find((c) => c.getAttribute("data-started-at"));
  assert.ok(timerElem, "Should have data-started-at attribute");
});

test("renderAssetList does not crash when fields are null or zero", () => {
  const readyList = document.getElementById("readyList");
  const incompleteAsset = {
    vod_uuid: "55555555-5555-5555-5555-555555555555",
    enlace_id: "INCOMPLETE-ASSET",
    status: "ready",
    duration_seconds: null,
    source_width: null,
    source_height: null,
    processing_time_seconds: null,
    processing_ratio: null,
    published_at: null,
  };

  assert.doesNotThrow(() => {
    renderAssetList([incompleteAsset], "readyList", "emptyReady", "readyTotal", false);
  });

  const row = readyList.children[0];
  const info = row.children.find((c) => c.classList.has("asset-info"));
  const texts = info.children.map((c) => c.textContent);
  assert.ok(texts.includes("—"));
});

test("renderAssetList handles FAILED asset without showing successful processing", () => {
  const failureList = document.getElementById("failureList");
  const failedAsset = {
    vod_uuid: "66666666-6666-6666-6666-666666666666",
    enlace_id: "FAILED-ASSET",
    status: "failed",
    duration_seconds: 1650.0,
    processing_started_at: "2026-09-24T10:00:00Z",
    processing_finished_at: "2026-09-24T10:12:44Z",
    processing_time_seconds: 764.0,
    processing_ratio: null,
  };

  renderAssetList([failedAsset], "failureList", "emptyFailures", "failureTotal", true);

  const row = failureList.children[0];
  const info = row.children.find((c) => c.classList.has("asset-info"));
  const texts = info.children.map((c) => c.textContent);
  assert.ok(texts.includes("Falló"));
  assert.ok(texts.includes("Falló después de 12m 44s"));
  assert.ok(texts.includes("—"));
});
