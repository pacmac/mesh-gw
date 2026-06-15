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
    msgChannel: "0",
    msgText: "",
    msgSent: false,
    messages: [],

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
      } catch (e) {
        sec.error = "Failed to load: " + e;
      } finally {
        sec.loading = false;
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
