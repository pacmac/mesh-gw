// mesh-rest-bridge dashboard
// Config/channel/owner forms are built dynamically from /schema/* +
// current values from /config/*, /channels/*, /owner -- adding a field to
// the protobuf shows up here automatically.

const SENSITIVE_FIELDS = new Set(["psk", "macaddr", "public_key", "private_key", "password"]);

function dashboard() {
  return {
    tab: "overview",
    status: {},
    info: { my_info: {}, metadata: {} },
    nodeSelf: {},
    nodes: [],
    nodeSort: { key: "last_heard", dir: -1 },
    mqttProxy: false,
    mqttCfg: {},
    loraCfg: {},
    wsConnected: false,
    events: [],
    allSections: [],
    channels: [],
    channelSchema: null,
    ownerSchema: null,
    ownerData: {},
    ownerSaved: false,
    ownerError: "",
    fixedPosition: { lat: null, lon: null, alt: null, loaded: false, saved: false, error: "" },
    msgChannel: "0",
    msgText: "",
    msgSent: false,
    messages: [],

    // BLE setup state
    bleDevices: [],
    bleScanning: false,
    bleConnecting: false,
    bleAddress: "",
    blePin: "",
    bleError: "",

    // Radar tab state
    radarRange: "100",
    radarNodes: [],
    radarSelected: null,
    homePos: null,
    geocoding: false,

    async init() {
      await this.refreshStatus();
      await this.loadInfo();
      await this.loadNodes();
      this.connectWS();
      setInterval(() => this.refreshStatus(), 5000);
    },

    // -- polling / status -----------------------------------------------------
    async refreshStatus() {
      this.status = await fetchJSON("/status");
      this.mqttProxy = this.status.mqtt_proxy_connected;
    },

    async loadInfo() {
      this.info = await fetchJSON("/info");
      const cfg = await fetchJSON("/config");
      this.mqttCfg = cfg.module_config?.mqtt || {};
      this.loraCfg = cfg.config?.lora || {};
    },

    async loadNodes() {
      const data = await fetchJSON("/nodes");
      this.nodes = Object.values(data.nodes || {});
      this.sortNodes(this.nodeSort.key, true);
      if (this.info.my_info?.my_node_num != null) {
        this.nodeSelf = data.nodes?.[String(this.info.my_info.my_node_num)] || {};
      }
    },

    // -- BLE management -------------------------------------------------------
    async bleScan() {
      this.bleScanning = true;
      this.bleError = "";
      try {
        const data = await fetchJSON("/ble/scan");
        this.bleDevices = data.devices || [];
        if (this.bleDevices.length === 0) this.bleError = "No Meshtastic devices found. Make sure the device is powered on and advertising.";
      } catch (e) {
        this.bleError = "Scan failed: " + (e.message || e);
      } finally {
        this.bleScanning = false;
      }
    },

    async bleConnect(address) {
      const addr = address || this.bleAddress;
      if (!addr) return;
      this.bleAddress = addr;
      this.bleConnecting = true;
      this.bleError = "";
      try {
        await fetch("/ble/connect", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ address: addr, pin: this.blePin || "" }),
        });
        // Poll ble_state until it leaves "connecting"
        const deadline = Date.now() + 60000;
        while (Date.now() < deadline) {
          await new Promise((r) => setTimeout(r, 2000));
          await this.refreshStatus();
          const s = this.status.ble_state;
          if (s !== "connecting") break;
        }
        if (this.status.ble_state === "error") {
          this.bleError = this.status.ble_error || "Connection failed";
        }
      } catch (e) {
        this.bleError = "Connect failed: " + (e.message || e);
      } finally {
        this.bleConnecting = false;
      }
    },

    async bleDisconnect() {
      this.bleError = "";
      try {
        await fetch("/ble/disconnect", { method: "POST" });
        // Poll until idle
        const deadline = Date.now() + 15000;
        while (Date.now() < deadline) {
          await new Promise((r) => setTimeout(r, 1000));
          await this.refreshStatus();
          if (this.status.ble_state === "idle") break;
        }
        this.bleDevices = [];
        this.bleAddress = "";
      } catch (e) {
        this.bleError = "Disconnect failed: " + (e.message || e);
      }
    },

    sortNodes(key, keepDir) {
      if (!keepDir) {
        this.nodeSort.dir = this.nodeSort.key === key ? -this.nodeSort.dir : -1;
      }
      this.nodeSort.key = key;
      const dir = this.nodeSort.dir;
      const getVal = (n) => {
        switch (key) {
          case "long_name": return n.user?.long_name || "";
          case "short_name": return n.user?.short_name || "";
          case "battery": return n.device_metrics?.battery_level ?? -1;
          case "snr": return n.snr ?? -999;
          case "hops": return n.hops ?? 999;
          case "last_heard": return n.last_heard ?? 0;
          default: return n[key] ?? "";
        }
      };
      this.nodes.sort((a, b) => {
        const av = getVal(a), bv = getVal(b);
        if (av < bv) return -1 * dir;
        if (av > bv) return 1 * dir;
        return 0;
      });
    },

    // -- websocket live feed ---------------------------------------------------
    connectWS() {
      const proto = location.protocol === "https:" ? "wss" : "ws";
      const ws = new WebSocket(`${proto}://${location.host}/ws`);
      ws.onopen = () => { this.wsConnected = true; };
      ws.onclose = () => {
        this.wsConnected = false;
        setTimeout(() => this.connectWS(), 3000);
      };
      ws.onerror = () => ws.close();
      ws.onmessage = (msg) => {
        const ev = JSON.parse(msg.data);
        this.handleEvent(ev);
      };
    },

    handleEvent(ev) {
      const time = new Date().toLocaleTimeString();
      const summary = summarizeEvent(ev);
      this.events.unshift({ type: ev.type, time, summary });
      if (this.events.length > 80) this.events.pop();

      if (ev.type === "packet") {
        const pkt = ev.data?.packet;
        const portnum = pkt?.decoded?.portnum;
        if (portnum === "TEXT_MESSAGE_APP" && pkt?.decoded?.payload) {
          try {
            const text = b64ToUtf8(pkt.decoded.payload);
            this.messages.unshift({ from: String(pkt.from ?? "?"), text, time });
            if (this.messages.length > 50) this.messages.pop();
          } catch (e) { /* ignore decode errors */ }
        }
        // Live-refresh node list occasionally on broadcasts
        if (this.tab === "nodes" && ["POSITION_APP", "NODEINFO_APP", "TELEMETRY_APP"].includes(portnum)) {
          this.loadNodes();
        }
      }
      if (ev.type === "config_complete_id" || ev.type === "node_info") {
        this.refreshStatus();
      }
      if (ev.type === "mqtt_node" && ev.data) {
        const upd = ev.data;
        const idx = this.nodes.findIndex((n) => n.num === upd.num);
        if (idx >= 0) this.nodes[idx] = { ...this.nodes[idx], ...upd };
        else this.nodes.push(upd);
        if (this.tab === "radar" && this.homePos && upd.position?.latitude_i) this.refreshRadar();
      }
    },

    // -- Radio Config tab --------------------------------------------------------
    async loadSections() {
      if (this.allSections.length) return;
      const sec = await fetchJSON("/sections");
      this.allSections = [
        ...sec.config.map((name) => ({ name, kind: "config", loaded: false, loading: false, saved: false, error: "" })),
        ...sec.module_config.map((name) => ({ name, kind: "module_config", loaded: false, loading: false, saved: false, error: "" })),
      ];
    },

    async onSectionToggle(sec) {
      if (sec.loaded || sec.loading) return;
      sec.loading = true;
      try {
        const [schema, values] = await Promise.all([
          fetchJSON(`/schema/${sec.name}`),
          fetchJSON(`/config/${sec.name}`),
        ]);
        sec.schema = schema;
        sec.data = values[sec.name] || values || {};
        await nextFrame();
        const el = document.getElementById("sec_" + sec.name);
        el.innerHTML = "";
        el.appendChild(buildForm(schema.fields, sec.data, []));
        sec.loaded = true;
        if (sec.name === "position" && !this.fixedPosition.loaded) await this.loadFixedPosition();
      } catch (e) {
        sec.error = "Failed to load: " + e;
      } finally {
        sec.loading = false;
      }
    },

    // -- Fixed position (lat/lon/alt, set via AdminMessage.set_fixed_position) ----
    async loadFixedPosition() {
      this.fixedPosition.error = "";
      try {
        const res = await fetchJSON("/fixed_position");
        const pos = res.position || {};
        this.fixedPosition.lat = pos.latitude_i != null ? pos.latitude_i / 1e7 : null;
        this.fixedPosition.lon = pos.longitude_i != null ? pos.longitude_i / 1e7 : null;
        this.fixedPosition.alt = pos.altitude ?? null;
        this.fixedPosition.loaded = true;
      } catch (e) {
        this.fixedPosition.error = "Failed to load: " + e;
      }
    },

    async saveFixedPosition() {
      this.fixedPosition.saved = false;
      this.fixedPosition.error = "";
      if (this.fixedPosition.lat == null || this.fixedPosition.lon == null) {
        this.fixedPosition.error = "Latitude and longitude are required";
        return;
      }
      const body = {
        latitude_i: Math.round(this.fixedPosition.lat * 1e7),
        longitude_i: Math.round(this.fixedPosition.lon * 1e7),
      };
      if (this.fixedPosition.alt != null && this.fixedPosition.alt !== "") {
        body.altitude = Math.round(this.fixedPosition.alt);
      }
      try {
        const res = await fetchJSON("/fixed_position", "PUT", body);
        if (res.error) throw new Error(res.error.message);
        this.fixedPosition.saved = true;
        setTimeout(() => (this.fixedPosition.saved = false), 2500);
      } catch (e) {
        this.fixedPosition.error = "Save failed: " + e;
      }
    },

    async clearFixedPosition() {
      this.fixedPosition.saved = false;
      this.fixedPosition.error = "";
      try {
        const res = await fetchJSON("/fixed_position", "DELETE");
        if (res.error) throw new Error(res.error.message);
        this.fixedPosition.lat = null;
        this.fixedPosition.lon = null;
        this.fixedPosition.alt = null;
        this.fixedPosition.saved = true;
        setTimeout(() => (this.fixedPosition.saved = false), 2500);
      } catch (e) {
        this.fixedPosition.error = "Clear failed: " + e;
      }
    },

    async saveSection(sec) {
      sec.saved = false;
      sec.error = "";
      const el = document.getElementById("sec_" + sec.name);
      const payload = collectForm(el, sec.schema.fields);
      try {
        const res = await fetchJSON(`/config/${sec.name}`, "PUT", payload);
        if (res.error) throw new Error(res.error.message);
        sec.saved = true;
        setTimeout(() => (sec.saved = false), 2500);
      } catch (e) {
        sec.error = "Save failed: " + e;
      }
    },

    // -- Channels tab -------------------------------------------------------------
    async loadChannels() {
      if (this.channels.length) return;
      this.channels = Array.from({ length: 8 }, (_, i) => ({
        index: i, loaded: false, loading: false, saved: false, error: "", data: {},
      }));
      // populate role/name badges from the cached full channel list
      try {
        const all = await fetchJSON("/channels");
        for (const ch of this.channels) {
          const c = all.channels?.[String(ch.index)];
          if (c) ch.data = c;
        }
      } catch (e) { /* ignore */ }
    },

    async onChannelToggle(ch) {
      if (ch.loaded || ch.loading) return;
      ch.loading = true;
      try {
        if (!this.channelSchema) this.channelSchema = await fetchJSON("/schema/channel");
        const live = await fetchJSON(`/channels/${ch.index}`);
        ch.data = live || {};
        await nextFrame();
        const el = document.getElementById("ch_" + ch.index);
        el.innerHTML = "";
        el.appendChild(buildForm(this.channelSchema.fields, ch.data, []));
        ch.loaded = true;
      } catch (e) {
        ch.error = "Failed to load: " + e;
      } finally {
        ch.loading = false;
      }
    },

    async saveChannel(ch) {
      ch.saved = false;
      ch.error = "";
      const el = document.getElementById("ch_" + ch.index);
      const payload = collectForm(el, this.channelSchema.fields);
      const body = { settings: { ...payload }, role: payload.role };
      delete body.settings.role;
      try {
        const res = await fetchJSON(`/channels/${ch.index}`, "PUT", body);
        if (res.error) throw new Error(res.error.message);
        ch.data = { ...ch.data, ...body };
        ch.saved = true;
        setTimeout(() => (ch.saved = false), 2500);
      } catch (e) {
        ch.error = "Save failed: " + e;
      }
    },

    // -- Owner tab ------------------------------------------------------------------
    async loadOwner() {
      if (this.ownerSchema) return;
      this.ownerSchema = await fetchJSON("/schema/owner");
      this.ownerData = await fetchJSON("/owner");
      await nextFrame();
      const el = document.getElementById("owner_form");
      el.innerHTML = "";
      // Only long_name / short_name / is_licensed are writable via set_owner;
      // show the rest read-only for context.
      const editable = ["long_name", "short_name", "is_licensed"];
      const editFields = this.ownerSchema.fields.filter((f) => editable.includes(f.name));
      const readonlyFields = this.ownerSchema.fields.filter((f) => !editable.includes(f.name));
      el.appendChild(buildForm(editFields, this.ownerData, []));
      const ro = document.createElement("div");
      ro.className = "divider text-xs";
      ro.textContent = "read-only";
      el.appendChild(ro);
      el.appendChild(buildForm(readonlyFields, this.ownerData, [], { readonly: true }));
    },

    async saveOwner() {
      this.ownerSaved = false;
      this.ownerError = "";
      const el = document.getElementById("owner_form");
      const payload = collectForm(el, this.ownerSchema.fields.filter((f) =>
        ["long_name", "short_name", "is_licensed"].includes(f.name)));
      try {
        const res = await fetchJSON("/owner", "PUT", payload);
        if (res.error) throw new Error(res.error.message);
        this.ownerSaved = true;
        setTimeout(() => (this.ownerSaved = false), 2500);
      } catch (e) {
        this.ownerError = "Save failed: " + e;
      }
    },

    // -- Messages tab ---------------------------------------------------------------
    async sendMessage() {
      if (!this.msgText.trim()) return;
      this.msgSent = false;
      await fetchJSON("/messages", "POST", { text: this.msgText, channel: Number(this.msgChannel) });
      this.msgText = "";
      this.msgSent = true;
      setTimeout(() => (this.msgSent = false), 2000);
    },

    // -- BLE signal bars -------------------------------------------------------------
    signalBarFill(n) {
      const snr = this.status.last_rx_snr;
      const bars = snr == null ? 1 : snr > 0 ? 4 : snr > -7 ? 3 : snr > -14 ? 2 : 1;
      if (n > bars) return "oklch(var(--bc)/0.12)";
      if (bars >= 4) return "oklch(var(--su))";
      if (bars >= 3) return "oklch(var(--su))";
      if (bars >= 2) return "oklch(var(--wa))";
      return "oklch(var(--er))";
    },

    // -- Radar tab -------------------------------------------------------------------
    async initRadar() {
      await this.loadNodes();
      const selfNum = this.info.my_info?.my_node_num;
      const self = selfNum != null ? this.nodes.find((n) => n.num === selfNum) : null;
      if (self?.position?.latitude_i) {
        this.homePos = {
          lat: self.position.latitude_i / 1e7,
          lon: self.position.longitude_i / 1e7,
        };
      } else {
        this.homePos = null;
        return;
      }
      this.refreshRadar();
      this.geocodeNodes();
    },

    refreshRadar() {
      if (!this.homePos) return;
      this.radarNodes = this.nodes
        .filter((n) => n.position?.latitude_i && n.position?.longitude_i)
        .map((n) => {
          const lat = n.position.latitude_i / 1e7;
          const lon = n.position.longitude_i / 1e7;
          const existing = this.radarNodes.find((r) => r.num === n.num);
          return {
            ...n,
            _km: haversine(this.homePos.lat, this.homePos.lon, lat, lon),
            _az: bearing(this.homePos.lat, this.homePos.lon, lat, lon),
            _lat: lat,
            _lon: lon,
            _address: existing?._address,
          };
        });
      this.drawRadar();
    },

    drawRadar() {
      const container = document.getElementById("radar-svg-container");
      if (!container || !this.homePos) return;
      container.innerHTML = "";
      const maxKm = this.radarRange === "0"
        ? (this.radarNodes.length ? Math.max(...this.radarNodes.map((n) => n._km)) * 1.15 : 50)
        : Number(this.radarRange);
      container.appendChild(buildRadarSVG(this.radarNodes, maxKm, (node) => {
        this.radarSelected = node;
      }));
    },

    async geocodeNodes() {
      this.geocoding = true;
      for (const node of [...this.radarNodes]) {
        if (node._address) continue;
        if (!node._lat || !node._lon) continue;
        const addr = await geocodeLatLon(node._lat, node._lon);
        node._address = addr;
        const idx = this.radarNodes.findIndex((r) => r.num === node.num);
        if (idx >= 0) this.radarNodes[idx] = { ...this.radarNodes[idx], _address: addr };
        if (this.radarSelected?.num === node.num) {
          this.radarSelected = { ...this.radarSelected, _address: addr };
        }
      }
      this.geocoding = false;
    },

    // -- formatting helpers -----------------------------------------------------------
    fmtUptime(secs) {
      if (secs == null) return "–";
      const d = Math.floor(secs / 86400);
      const h = Math.floor((secs % 86400) / 3600);
      const m = Math.floor((secs % 3600) / 60);
      if (d > 0) return `${d}d ${h}h`;
      if (h > 0) return `${h}h ${m}m`;
      return `${m}m`;
    },
    fmtBytes(b) {
      if (b == null) return "–";
      if (b > 1024) return (b / 1024).toFixed(1) + " KB";
      return b + " B";
    },
    dewPoint(tempC, rh) {
      if (tempC == null || rh == null || rh <= 0) return null;
      const a = 17.27, b = 237.7;
      const alpha = (a * tempC) / (b + tempC) + Math.log(rh / 100);
      return (b * alpha) / (a - alpha);
    },
    fmtAge(ts) {
      if (!ts) return "–";
      const secs = Math.floor(Date.now() / 1000) - ts;
      if (secs < 60) return secs + "s ago";
      if (secs < 3600) return Math.floor(secs / 60) + "m ago";
      if (secs < 86400) return Math.floor(secs / 3600) + "h ago";
      return Math.floor(secs / 86400) + "d ago";
    },
    badgeForType(type) {
      switch (type) {
        case "packet": return "badge-primary";
        case "node_info": return "badge-secondary";
        case "config_complete_id": return "badge-success";
        case "mqttClientProxyMessage": return "badge-accent";
        default: return "badge-ghost";
      }
    },
  };
}

// ============================================================================
// Dynamic form building / collection from /schema/* responses
// ============================================================================

function buildForm(fields, data, path, opts = {}) {
  const wrap = document.createElement("div");
  wrap.className = "grid grid-cols-1 sm:grid-cols-2 gap-3";
  for (const field of fields) {
    wrap.appendChild(buildField(field, data?.[field.name], path.concat(field.name), opts));
  }
  return wrap;
}

function buildField(field, value, path, opts = {}) {
  const fieldPath = path.join(".");

  if (field.type === "object") {
    const box = document.createElement("div");
    box.className = "col-span-1 sm:col-span-2 border border-base-300 rounded-lg p-3";
    const title = document.createElement("div");
    title.className = "text-xs font-semibold uppercase text-base-content/50 mb-2";
    title.textContent = field.name.replace(/_/g, " ");
    box.appendChild(title);
    box.appendChild(buildForm(field.fields, value || {}, path, opts));
    return box;
  }

  const ctl = document.createElement("label");
  ctl.className = "form-control w-full";
  const labelRow = document.createElement("div");
  labelRow.className = "label py-1";
  const labelText = document.createElement("span");
  labelText.className = "label-text text-xs";
  labelText.textContent = field.name.replace(/_/g, " ") + (field.repeated ? " (comma separated)" : "");
  labelRow.appendChild(labelText);
  ctl.appendChild(labelRow);

  const sensitive = SENSITIVE_FIELDS.has(field.name);
  let input;

  if (field.type === "bool" && !field.repeated) {
    input = document.createElement("input");
    input.type = "checkbox";
    input.className = "toggle toggle-primary toggle-sm";
    input.checked = !!value;
    input.dataset.field = fieldPath;
    input.dataset.type = field.type;
    ctl.classList.add("flex-row", "items-center", "justify-between");
    ctl.style.flexDirection = "row";
    ctl.style.alignItems = "center";
    ctl.style.justifyContent = "space-between";
  } else if (field.type === "enum" && !field.repeated) {
    input = document.createElement("select");
    input.className = "select select-bordered select-sm w-full";
    for (const opt of field.options) {
      const o = document.createElement("option");
      o.value = opt;
      o.textContent = opt;
      if (opt === value) o.selected = true;
      input.appendChild(o);
    }
    input.dataset.field = fieldPath;
    input.dataset.type = field.type;
  } else {
    input = document.createElement("input");
    input.className = "input input-bordered input-sm w-full font-mono";
    input.dataset.field = fieldPath;
    input.dataset.type = field.type;
    input.dataset.repeated = field.repeated ? "1" : "";

    if (field.repeated) {
      input.type = "text";
      input.value = Array.isArray(value) ? value.join(", ") : "";
    } else if (field.type === "int") {
      input.type = "number";
      input.step = "1";
      input.value = value ?? 0;
    } else if (field.type === "float") {
      input.type = "number";
      input.step = "any";
      input.value = value ?? 0;
    } else {
      input.type = sensitive ? "password" : "text";
      input.value = value ?? "";
    }

    if (sensitive && !opts.readonly) {
      // Locked by default -- prevents accidental overwrite of psk/keys,
      // which silently wipes the field if blanked and saved (SetChannel /
      // SetConfig replace the whole sub-message, not a merge).
      input.disabled = true;
      const unlock = document.createElement("label");
      unlock.className = "label cursor-pointer gap-1 py-0";
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.className = "checkbox checkbox-xs";
      cb.addEventListener("change", () => { input.disabled = !cb.checked; if (cb.checked) input.type = "text"; else input.type = "password"; });
      const lbl = document.createElement("span");
      lbl.className = "label-text text-xs text-warning";
      lbl.textContent = "unlock to edit";
      unlock.appendChild(cb);
      unlock.appendChild(lbl);
      labelRow.appendChild(unlock);
    }
  }

  if (opts.readonly) {
    input.disabled = true;
    if (input.tagName === "SELECT") input.classList.add("opacity-60");
  }

  ctl.appendChild(input);
  return ctl;
}

function collectForm(container, fields) {
  return collectFromInputs(container, fields, []);
}

function collectFromInputs(container, fields, path) {
  const out = {};
  for (const field of fields) {
    const p = path.concat(field.name);
    if (field.type === "object") {
      out[field.name] = collectFromInputs(container, field.fields, p);
      continue;
    }
    const sel = `[data-field="${p.join(".")}"]`;
    const input = container.querySelector(sel);
    if (!input) continue;
    if (input.disabled) continue; // locked sensitive field: don't send, leave unchanged server-side... but SetConfig replaces whole message!
    out[field.name] = readFieldValue(input, field);
  }
  return out;
}

function readFieldValue(input, field) {
  if (field.type === "bool" && !field.repeated) return input.checked;
  if (field.repeated) {
    const raw = input.value.split(",").map((s) => s.trim()).filter((s) => s !== "");
    if (field.type === "int") return raw.map(Number);
    if (field.type === "float") return raw.map(Number);
    return raw;
  }
  if (field.type === "int") return parseInt(input.value, 10) || 0;
  if (field.type === "float") return parseFloat(input.value) || 0;
  return input.value;
}

// ============================================================================
// misc helpers
// ============================================================================

async function fetchJSON(url, method = "GET", body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(url, opts);
  return res.json();
}

function b64ToUtf8(b64) {
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return new TextDecoder("utf-8").decode(bytes);
}

function nextFrame() {
  return new Promise((resolve) => requestAnimationFrame(resolve));
}

// ============================================================================
// Radar helpers
// ============================================================================

function haversine(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.asin(Math.sqrt(a));
}

function bearing(lat1, lon1, lat2, lon2) {
  const y = Math.sin((lon2 - lon1) * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180);
  const x = Math.cos(lat1 * Math.PI / 180) * Math.sin(lat2 * Math.PI / 180) -
    Math.sin(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.cos((lon2 - lon1) * Math.PI / 180);
  return (Math.atan2(y, x) * 180 / Math.PI + 360) % 360;
}

function svgElem(tag, attrs = {}) {
  const el = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "style") { el.style.cssText = v; }
    else el.setAttribute(k, v);
  }
  return el;
}

function buildRadarSVG(nodes, maxKm, onSelect) {
  const SIZE = 480;
  const CX = SIZE / 2, CY = SIZE / 2, R = SIZE / 2 - 36;

  const svg = svgElem("svg", { viewBox: `0 0 ${SIZE} ${SIZE}`, width: "100%", style: "max-width:480px;display:block;margin:auto;overflow:visible" });

  // Background
  svg.appendChild(svgElem("circle", { cx: CX, cy: CY, r: R, style: "fill:oklch(var(--b2)/0.6);stroke:oklch(var(--bc)/0.10);stroke-width:1" }));

  // Concentric rings
  for (let i = 1; i <= 4; i++) {
    const r = R * i / 4;
    const dash = i < 4 ? "4 6" : "";
    svg.appendChild(svgElem("circle", { cx: CX, cy: CY, r, style: `fill:none;stroke:oklch(var(--bc)/0.12);stroke-width:1;stroke-dasharray:${dash}` }));
    const lbl = svgElem("text", { x: CX + 4, y: CY - r + 13, style: "fill:oklch(var(--bc)/0.35);font-size:10px;font-family:monospace" });
    lbl.textContent = (maxKm * i / 4).toFixed(0) + " km";
    svg.appendChild(lbl);
  }

  // Cross-hairs
  svg.appendChild(svgElem("line", { x1: CX, y1: CY - R, x2: CX, y2: CY + R, style: "stroke:oklch(var(--bc)/0.08);stroke-width:0.5" }));
  svg.appendChild(svgElem("line", { x1: CX - R, y1: CY, x2: CX + R, y2: CY, style: "stroke:oklch(var(--bc)/0.08);stroke-width:0.5" }));

  // Compass labels
  for (const [label, dx, dy] of [["N", 0, -1], ["E", 1, 0], ["S", 0, 1], ["W", -1, 0]]) {
    const t = svgElem("text", {
      x: CX + dx * (R + 18),
      y: CY + dy * (R + 18) + 4,
      style: "fill:oklch(var(--bc)/0.55);font-size:12px;font-weight:700;font-family:monospace;text-anchor:middle",
    });
    t.textContent = label;
    svg.appendChild(t);
  }

  // Node dots
  for (const node of nodes) {
    const az = node._az * Math.PI / 180;
    const normKm = Math.min(node._km / maxKm, 1.0);
    const x = CX + Math.sin(az) * normKm * R;
    const y = CY - Math.cos(az) * normKm * R;

    const snr = node.snr;
    let dotColor;
    if (snr == null)     dotColor = "oklch(var(--bc)/0.35)";
    else if (snr > -7)   dotColor = "oklch(var(--su))";
    else if (snr > -14)  dotColor = "oklch(var(--wa))";
    else                 dotColor = "oklch(var(--er))";

    const g = svgElem("g", { style: "cursor:pointer" });
    g.appendChild(svgElem("circle", { cx: x, cy: y, r: 6, style: `fill:${dotColor};stroke:oklch(var(--b1));stroke-width:1.5` }));

    const label = node.user?.short_name || ("!" + (node.num ?? 0).toString(16).slice(-4));
    const txt = svgElem("text", {
      x: x + 8, y: y + 4,
      style: "fill:oklch(var(--bc)/0.75);font-size:9px;font-family:monospace;pointer-events:none",
    });
    txt.textContent = label;
    g.appendChild(txt);
    g.addEventListener("click", (e) => { e.stopPropagation(); onSelect(node); });
    svg.appendChild(g);
  }

  // Home marker
  const hg = svgElem("g");
  hg.appendChild(svgElem("circle", { cx: CX, cy: CY, r: 8, style: "fill:oklch(var(--p));stroke:oklch(var(--b1));stroke-width:2" }));
  hg.appendChild(svgElem("line", { x1: CX - 12, y1: CY, x2: CX + 12, y2: CY, style: "stroke:oklch(var(--b1));stroke-width:1.5" }));
  hg.appendChild(svgElem("line", { x1: CX, y1: CY - 12, x2: CX, y2: CY + 12, style: "stroke:oklch(var(--b1));stroke-width:1.5" }));
  svg.appendChild(hg);

  return svg;
}

// ============================================================================
// Nominatim geocoding — sequential queue with 1.1s inter-request delay
// ============================================================================

const _geocodeCache = new Map();
let _geocodeQueue = Promise.resolve();

function geocodeLatLon(lat, lon) {
  const key = lat.toFixed(4) + "," + lon.toFixed(4);
  if (_geocodeCache.has(key)) return Promise.resolve(_geocodeCache.get(key));

  const p = _geocodeQueue.then(
    () =>
      new Promise((resolve) => {
        setTimeout(async () => {
          try {
            const url = `https://nominatim.openstreetmap.org/reverse?lat=${lat}&lon=${lon}&format=json&zoom=10`;
            const res = await fetch(url, { headers: { "Accept-Language": "en" } });
            const data = await res.json();
            const parts = (data.display_name || "").split(",").map((s) => s.trim());
            const addr = parts.slice(0, 3).join(", ") || `${lat.toFixed(4)}, ${lon.toFixed(4)}`;
            _geocodeCache.set(key, addr);
            resolve(addr);
          } catch (_) {
            const fallback = `${lat.toFixed(4)}, ${lon.toFixed(4)}`;
            _geocodeCache.set(key, fallback);
            resolve(fallback);
          }
        }, 1100);
      })
  );

  _geocodeQueue = p;
  return p;
}

// ============================================================================
// misc helpers
// ============================================================================

function summarizeEvent(ev) {
  switch (ev.type) {
    case "packet": {
      const pkt = ev.data?.packet;
      const portnum = pkt?.decoded?.portnum || "?";
      return `from !${(pkt?.from ?? 0).toString(16)} -> ${portnum}`;
    }
    case "node_info": {
      const u = ev.data?.node_info?.user;
      return u ? `${u.long_name} (${u.id})` : "node update";
    }
    case "config_complete_id":
      return "NodeDB sync complete";
    case "mqttClientProxyMessage": {
      const m = ev.data?.mqttClientProxyMessage;
      return m?.topic || "";
    }
    default:
      return "";
  }
}
