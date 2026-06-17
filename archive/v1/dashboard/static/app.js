// mesh-gw dashboard
// Config/channel/owner forms are built dynamically from /schema/* +
// current values from /config/*, /channels/*, /owner -- adding a field to
// the protobuf shows up here automatically.

const SENSITIVE_FIELDS = new Set(["psk", "macaddr", "public_key", "private_key", "password"]);

function dashboard() {
  return {
    tab: localStorage.getItem("activeTab") || "overview",
    cfgTab: localStorage.getItem("activeCfgTab") || "device",
    drawerOpen: false,
    sidebarPinned: localStorage.getItem('sidebarPinned') !== 'false',
    activeNodeId: localStorage.getItem("activeNodeId") || "",
    msgFrom: localStorage.getItem("msgFrom") || "",
    msgIsDirect: false,
    msgDirectTo: "",
    msgInsertNode: null,
    msgInputHistory: JSON.parse(localStorage.getItem('msgInputHistory') || '[]'),
    msgHistoryIdx: -1,
    msgDraft: '',
    mentionOpen: false,
    mentionQuery: '',
    mentionPos: 0,
    mentionIdx: 0,
    unreadMessages: 0,
    _seenPacketIds: new Set(),
    availableDevices: [],
    status: {},
    info: { my_info: {}, metadata: {} },
    nodeSelf: {},
    nodes: [],
    nodeTotal: 0,
    nodeCount: 0,
    nodeSort: { key: "last_heard", dir: -1 },
    nodeFilters: {
      maxHops:    parseInt(localStorage.getItem("nf_maxHops")   ?? "99"),
      namedOnly:  localStorage.getItem("nf_namedOnly")  === "1",
      hasPos:     localStorage.getItem("nf_hasPos")     === "1",
      hasSignal:  localStorage.getItem("nf_hasSignal")  === "1",
      hasTelem:   localStorage.getItem("nf_hasTelem")   === "1",
      maxAge:     parseInt(localStorage.getItem("nf_maxAge")    ?? "0"),
      hideMqtt:   localStorage.getItem("nf_hideMqtt") !== "0",  // default ON
      nodeRoles:  JSON.parse(localStorage.getItem("nf_nodeRoles") || "[]"),
    },
    mqttProxy: false,
    mqttCfg: {},
    loraCfg: {},
    wsConnected: false,
    serverReachable: true,
    yagiAz: null,
    yagiConnected: false,
    yagiPointTarget: null,
    rotatorStatus: {},
    rotatorConnected: false,
    rotatorManualAz: null,
    rotatorMode: 0,
    yagiRadioAddr: '',
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
    bleAutoConnect: true,
    bleError: "",

    // Radar tab state
    radarRange: localStorage.getItem("radarRange") || "100",
    radarLogScale: localStorage.getItem("radarLogScale") !== "false",
    radarNodes: [],
    radarSelected: null,
    homePos: JSON.parse(localStorage.getItem("homePos") || "null"),
    geocoding: false,
    radarCrosshair: localStorage.getItem("radarCrosshair") !== "false",
    heatmapMaxAge: Number(localStorage.getItem("heatmapMaxAge")) || 3600,

    // Node Info modal (reusable, callable from Radar and Nodes table)
    nodeInfo: null,

    get targetNode() {
      if (!this.yagiPointTarget) return null;
      // radarNodes has enriched _km/_az; fall back to raw nodes if not plotted
      return this.radarNodes.find(n => n.num === this.yagiPointTarget)
          || this.nodes.find(n => n.num === this.yagiPointTarget)
          || null;
    },
    lastHeardNum: null,

    // Bridge Config tab state (core/bridge_config.yaml via /bridge_config)
    bridgeConfig: null,
    bridgeConfigSaved: false,
    bridgeConfigError: "",

    // Range test tab state
    rangeLog: [],
    rangeLoading: false,

    msgIsModal: false,
    _composeTa: null,

    // -- nav -----------------------------------------------------------------------
    setNav(t, c) {
      this.tab = t;
      localStorage.setItem("activeTab", t);
      if (c) { this.cfgTab = c; localStorage.setItem("activeCfgTab", c); }
      this.drawerOpen = false;
      if (t === "radar") this.$nextTick(() => this.initRadar());
      else if (t === "cfg") this.switchCfgTab(c || this.cfgTab);
      else if (t === "range") this.loadRangeTest();
      else if (t === "nodes") this.loadNodes();
      else if (t === "devices") this.loadDevices();
      else if (t === "messages") this.unreadMessages = 0;
    },

    // -- device path helper -------------------------------------------------------
    d(path) {
      return this.activeNodeId ? "/" + this.activeNodeId + path : path;
    },

    // -- device management -------------------------------------------------------
    async loadDevices() {
      try {
        const data = await fetchJSON("/devices");
        this.availableDevices = (data.devices || []).filter(d => !String(d.node_id).startsWith("ble:"));
        if (!this.activeNodeId && this.availableDevices.length > 0) {
          this.activeNodeId = this.availableDevices[0].node_id;
          localStorage.setItem("activeNodeId", this.activeNodeId);
        }
        if (!this.msgFrom || !this.availableDevices.find(d => d.node_id === this.msgFrom)) {
          this.msgFrom = this.activeNodeId;
          localStorage.setItem("msgFrom", this.msgFrom);
        }
      } catch (e) {
        console.warn("Failed to load devices", e);
      }
    },

    async selectDevice(nodeId) {
      this.activeNodeId = nodeId;
      localStorage.setItem("activeNodeId", nodeId);
      this.allSections = [];
      this.channels = [];
      this.ownerSchema = null;
      this.reconnectWS();
      await this.refreshStatus();
      await this.loadInfo();
      await this.loadNodes();
      this.updateHomePos();
      if (this.tab === "radar") this.initRadar();
    },

    async disconnectDevice(nodeId) {
      this.bleError = "";
      try {
        await fetchJSON("/devices/" + encodeURIComponent(nodeId), "DELETE");
        if (this.activeNodeId === nodeId) this.activeNodeId = "";
        await this.loadDevices();
        if (!this.activeNodeId && this.availableDevices.length > 0) {
          await this.selectDevice(this.availableDevices[0].node_id);
        }
      } catch (e) {
        this.bleError = "Disconnect failed: " + e;
      }
    },

    async init() {
      try {
        const saved = JSON.parse(localStorage.getItem("msgHistory") || "[]");
        if (Array.isArray(saved) && saved.length) {
          this.messages = saved;
          saved.forEach(m => { if (m.pktId) this._seenPacketIds.add(m.pktId); });
        }
      } catch (_) {}
      await this.loadDevices();
      if (this.activeNodeId) {
        await this.refreshStatus();
        await this.loadInfo();
        await this.loadNodes();
        this.updateHomePos();
      }
      await this.loadBridgeConfig();
      await this.loadRotatorState();
      this.connectWS();
      setInterval(() => { this.refreshStatus(); this.loadDevices(); }, 5000);
      if (this.tab === "radar") this.$nextTick(() => this.initRadar());
      else if (this.tab === "cfg") this.switchCfgTab(this.cfgTab);
      else if (this.tab === "range") this.loadRangeTest();
    },

    // -- bridge-side config (core/bridge_config.yaml via /bridge_config) ------
    // Radar UI prefs (max range, crosshair, heatmap max-age) and MQTT topic
    // conventions; localStorage is kept as an offline fallback/cache.
    async loadBridgeConfig() {
      try {
        this.bridgeConfig = await fetchJSON("/bridge_config");
        this.radarRange = String(this.bridgeConfig.radar.max_range_km);
        this.radarCrosshair = this.bridgeConfig.radar.crosshair_default;
        this.heatmapMaxAge = this.bridgeConfig.radar.heatmap_max_age_sec;
        localStorage.setItem("radarRange", this.radarRange);
        localStorage.setItem("radarCrosshair", this.radarCrosshair);
        localStorage.setItem("heatmapMaxAge", this.heatmapMaxAge);
        // Load active node filter from server — single source of truth shared with rotator
        try {
          const nf = await fetchJSON("/node_filter");
          this.nodeFilters = {
            maxAge:    nf.max_age      ?? this.nodeFilters.maxAge,
            maxHops:   nf.max_hops     ?? this.nodeFilters.maxHops,
            namedOnly: nf.named_only   ?? this.nodeFilters.namedOnly,
            hasPos:    nf.has_position ?? this.nodeFilters.hasPos,
            hideMqtt:  nf.hide_mqtt    ?? this.nodeFilters.hideMqtt,
            hasSignal: nf.has_signal    ?? this.nodeFilters.hasSignal,
            hasTelem:  nf.has_telemetry ?? this.nodeFilters.hasTelem,
            nodeRoles: nf.node_roles    ?? this.nodeFilters.nodeRoles,
          };
        } catch (_) {}
        // Resolve yagi radio assignment from devices block
        const devs = this.bridgeConfig.devices || {};
        this.yagiRadioAddr = (Object.entries(devs)
          .find(([, v]) => v?.role === 'yagi')?.[0] || '').toUpperCase();
      } catch (e) {
        console.warn("Failed to load bridge config, using localStorage fallback", e);
      }
    },

    resetNodeFilters() {
      const nd = this.bridgeConfig?.node_display || {};
      this.nodeFilters = {
        maxHops:   99,
        namedOnly: false,
        hasPos:    false,
        hasSignal: false,
        hasTelem:  false,
        maxAge:    nd.default_max_age_sec ?? 0,
        hideMqtt:  nd.default_hide_mqtt   ?? true,
        nodeRoles: [],
      };
      localStorage.removeItem("nf_nodeRoles");
      fetchJSON("/node_filter", "PUT", {
        max_age: this.nodeFilters.maxAge, max_hops: 99,
        named_only: false, has_position: false,
        hide_mqtt: this.nodeFilters.hideMqtt,
        has_signal: false, has_telemetry: false,
        node_roles: [],
      }).catch(() => {});
    },

    async saveBridgeConfig() {
      this.bridgeConfigSaved = false;
      this.bridgeConfigError = "";
      try {
        this.bridgeConfig = await fetchJSON("/bridge_config", "PUT", this.bridgeConfig);
        this.bridgeConfigSaved = true;
        setTimeout(() => { this.bridgeConfigSaved = false; }, 2000);
      } catch (e) {
        this.bridgeConfigError = String(e);
      }
    },

    // Persist a single radar pref (called on change from the Radar tab).
    saveRadarPref(key, value) {
      localStorage.setItem(key === "max_range_km" ? "radarRange" : key, value);
      if (!this.bridgeConfig) return;
      this.bridgeConfig.radar[key] = value;
      fetchJSON("/bridge_config", "PUT", { radar: { [key]: value } }).catch((e) =>
        console.warn("Failed to save radar pref", e));
    },

    // -- home position (self GPS fix), used by radar + nodes table ------------
    updateHomePos() {
      const selfNum = this.info.my_info?.my_node_num;
      const self = selfNum != null ? this.nodes.find((n) => n.num === selfNum) : null;
      if (self?.position?.latitude_i) {
        this.homePos = {
          lat: self.position.latitude_i / 1e7,
          lon: self.position.longitude_i / 1e7,
        };
        localStorage.setItem("homePos", JSON.stringify(this.homePos));
      } else if (!this.homePos) {
        this.homePos = null; // only clear if no cached value
      }
    },

    // KM/AZ from home to a node (for the Nodes table); null if unknown.
    nodeKm(n) {
      if (!this.homePos || !n.position?.latitude_i || !n.position?.longitude_i) return null;
      return haversine(this.homePos.lat, this.homePos.lon, n.position.latitude_i / 1e7, n.position.longitude_i / 1e7);
    },
    nodeAz(n) {
      if (!this.homePos || !n.position?.latitude_i || !n.position?.longitude_i) return null;
      return bearing(this.homePos.lat, this.homePos.lon, n.position.latitude_i / 1e7, n.position.longitude_i / 1e7);
    },
    rssiPercent(rssi) {
      return rssiPercent(rssi);
    },

    // -- polling / status -----------------------------------------------------
    async refreshStatus() {
      if (!this.activeNodeId) return;
      try {
        this.status = await fetchJSON(this.d("/status"));
        this.mqttProxy = this.status.mqtt_proxy_connected;
        this.serverReachable = true;
      } catch (_) {
        this.serverReachable = false;
      }
    },

    async loadInfo() {
      if (!this.activeNodeId) return;
      this.info = await fetchJSON(this.d("/info"));
      const cfg = await fetchJSON(this.d("/config"));
      this.mqttCfg = cfg.module_config?.mqtt || {};
      this.loraCfg = cfg.config?.lora || {};
    },

    async loadNodes() {
      if (!this.availableDevices.length) return;
      const f = this.nodeFilters;
      const qs = new URLSearchParams();
      if (f.maxAge)           qs.set("max_age",      f.maxAge);
      if (f.maxHops < 99)     qs.set("max_hops",     f.maxHops);
      if (f.namedOnly)        qs.set("named_only",   "true");
      if (f.hasPos)           qs.set("has_position", "true");
      if (f.hideMqtt)         qs.set("hide_mqtt",    "true");
      if (f.hasSignal)        qs.set("has_signal",   "true");
      if (f.hasTelem)         qs.set("has_telemetry","true");
      if (f.nodeRoles?.length) f.nodeRoles.forEach(r => qs.append("node_roles", r));
      const query = qs.toString() ? "?" + qs.toString() : "";
      // /nodes aggregates all bridges server-side — same dataset the rotator uses.
      const data = await fetchJSON("/nodes" + query);
      this.nodeTotal = data.total ?? Object.keys(data.nodes || {}).length;
      this.nodeCount = data.count ?? this.nodeTotal;
      this.nodes = Object.values(data.nodes || {});
      this.updateHomePos();
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
        await fetchJSON("/devices", "POST", { address: addr, pin: this.blePin || "" });
        // Poll devices until the new one appears and is ready
        const deadline = Date.now() + 60000;
        while (Date.now() < deadline) {
          await new Promise((r) => setTimeout(r, 2000));
          await this.loadDevices();
          const dev = this.availableDevices.find(d => d.ble_address?.toUpperCase() === addr.toUpperCase());
          if (dev) {
            if (!this.activeNodeId) await this.selectDevice(dev.node_id);
            if (dev.ble_state !== "connecting") break;
          }
        }
      } catch (e) {
        this.bleError = "Connect failed: " + (e.message || e);
      } finally {
        this.bleConnecting = false;
      }
    },

    async bleDisconnect() {
      this.bleError = "";
      if (!this.activeNodeId) return;
      try {
        await this.disconnectDevice(this.activeNodeId);
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
          case "id": return n.user?.id || String(n.num ?? 0);
          case "battery": return n.device_metrics?.battery_level ?? -1;
          case "snr": return n.snr ?? -999;
          case "rssi": return this.rssiPercent ? rssiPercent(n.rssi ?? -999) : (n.rssi ?? -999);
          case "km": return this.nodeKm(n) ?? 9999;
          case "az": return this.nodeAz(n) ?? -1;
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

    saveNodeFilter(key, val) {
      this.nodeFilters[key] = val;
      if (key === 'nodeRoles') localStorage.setItem("nf_nodeRoles", JSON.stringify(val));
      fetchJSON("/node_filter", "PUT", {
        max_age:      this.nodeFilters.maxAge,
        max_hops:     this.nodeFilters.maxHops,
        named_only:   this.nodeFilters.namedOnly,
        has_position: this.nodeFilters.hasPos,
        hide_mqtt:    this.nodeFilters.hideMqtt,
        has_signal:   this.nodeFilters.hasSignal,
        has_telemetry: this.nodeFilters.hasTelem,
        node_roles:   this.nodeFilters.nodeRoles,
      }).catch(() => {});
      this.loadNodes();
    },

    toggleNodeRole(role, checked) {
      const ALL = ['CLIENT','CLIENT_BASE','CLIENT_MUTE','ROUTER','ROUTER_CLIENT','ROUTER_LATE','TRACKER','SENSOR','REPEATER'];
      let cur = this.nodeFilters.nodeRoles.length ? [...this.nodeFilters.nodeRoles] : [...ALL];
      cur = checked ? [...new Set([...cur, role])] : cur.filter(r => r !== role);
      this.saveNodeFilter('nodeRoles', cur.length === ALL.length ? [] : cur);
    },

    filteredNodes() {
      const bridgeNums = new Set(
        this.availableDevices
          .map(d => d.node_id?.startsWith('!') ? parseInt(d.node_id.slice(1), 16) : null)
          .filter(n => n != null && !isNaN(n))
      );
      return bridgeNums.size ? this.nodes.filter(n => !bridgeNums.has(n.num >>> 0)) : this.nodes;
    },

    // -- websocket live feed ---------------------------------------------------
    connectWS() {
      const apiUrl = window.MESH_API || location.origin;
      // No device filter — rotator events come from the YAGI bridge which may differ
      // from the active device; JS-side filtering handles per-device dispatch.
      const ws = new WebSocket(apiUrl.replace(/^http/, "ws") + "/events");
      this._ws = ws;
      ws.onopen = () => { this.wsConnected = true; };
      ws.onclose = () => {
        this.wsConnected = false;
        if (ws === this._ws) setTimeout(() => this.connectWS(), 3000);
      };
      ws.onerror = () => ws.close();
      ws.onmessage = (msg) => {
        const ev = JSON.parse(msg.data);
        this.handleEvent(ev);
      };
    },

    reconnectWS() {
      if (this._ws) { this._ws.onclose = null; this._ws.close(); this._ws = null; }
      this.wsConnected = false;
      this.connectWS();
    },

    // -- Rotator (status via bridge WS type:rotator events) ------------------
    _onRotatorEvent(data) {
      this.rotatorConnected = true;
      if (data.az != null) {
        const azChanged = this.yagiAz !== data.az;
        this.yagiAz = data.az;
        this.yagiConnected = true;
        if (data.evt != null) this.rotatorStatus = data; // hardware status only
      }
      if ('point_target' in data) {
        this.yagiPointTarget = data.point_target;
        if (this.tab === "radar") this.drawRadar(); // rebuild for crosshairs
      }
      // Animate beam after any potential rebuild — always last
      if (data.az != null && this.tab === "radar") this._animateBeam(data.az);
    },

    // Update beam transform — element is persistent across SVG rebuilds so
    // the CSS transition state is preserved and the browser sweeps smoothly
    _animateBeam(newAz) {
      const beamG = document.getElementById('radar-beam-g');
      if (!beamG) return;
      if (!beamG.firstChild) this._drawRadarBeam();
      if (this._radarBeamAz == null) {
        // First position — snap without sweeping from north
        beamG.style.transition = 'none';
        beamG.style.transform = `rotate(${newAz}deg)`;
        beamG.getBoundingClientRect(); // commit style before re-enabling transition
        beamG.style.transition = 'transform 1.2s ease';
      } else {
        beamG.style.transform = `rotate(${newAz}deg)`;
      }
      this._radarBeamAz = newAz;
      const lbl = beamG.querySelector('text');
      if (lbl) lbl.textContent = Math.round(newAz) + '°';
    },
    async loadRotatorState() {
      try {
        const d = await fetchJSON("/rotator/state");
        this.rotatorMode = d.mode ?? 0;
        this.yagiPointTarget = d.point_target ?? null;
      } catch (_) {}
    },
    async setRotatorMode(m) {
      this.rotatorMode = m;
      try { await fetchJSON("/rotator/state", "POST", { mode: m }); } catch (_) {}
    },
    async moveRotator(az) {
      if (az == null) return;
      await fetchJSON("/rotator/move", "POST", { az: Number(az) });
    },

    handleEvent(ev) {
      // Rotator events come from the YAGI bridge (not necessarily the active device).
      if (ev.type === "rotator") { this._onRotatorEvent(ev.data || {}); return; }

      // Position/nodeinfo from any bridge: refresh /nodes (aggregated) so nodes
      // heard only by EG5y appear on the radar without a page reload.
      if (ev.type === "packet") {
        const portnum = ev.data?.packet?.decoded?.portnum;
        if (portnum === "POSITION_APP" || portnum === "NODEINFO_APP")
          this.loadNodes().then(() => { if (this.tab === "radar") this.refreshRadar(); });
      }

      // Everything else: filter to the active device.
      if (ev.device && this.activeNodeId && ev.device !== this.activeNodeId) return;
      const time = new Date().toLocaleTimeString();
      const summary = summarizeEvent(ev);
      this.events.unshift({ type: ev.type, time, summary, device: ev.device || null });
      if (this.events.length > 80) this.events.pop();

      if (ev.type === "packet") {
        const pkt = ev.data?.packet;
        const portnum = pkt?.decoded?.portnum;
        if (portnum === "TEXT_MESSAGE_APP" && pkt?.decoded?.payload) {
          try {
            const pktId = pkt.id;
            if (!pktId || !this._seenPacketIds.has(pktId)) {
              if (pktId) { this._seenPacketIds.add(pktId); if (this._seenPacketIds.size > 200) this._seenPacketIds.delete(this._seenPacketIds.values().next().value); }
              const text = b64ToUtf8(pkt.decoded.payload);
              const toNum = pkt.to >>> 0;
              this.messages.unshift({
                pktId, fromNum: pkt.from ?? 0, to: toNum,
                broadcast: toNum === 0xFFFFFFFF || pkt.to == null,
                channel: pkt.channel ?? 0,
                replyId: pkt.decoded.reply_id || null,
                text, time, direction: 'rx', ackStatus: null,
                src: ev.device || null,
              });
              if (this.messages.length > 50) this.messages.pop();
              try { localStorage.setItem("msgHistory", JSON.stringify(this.messages.slice(0, 20))); } catch (_) {}
              if (this.tab !== 'messages') { this.unreadMessages++; this.playMsgSound(); }
            }
          } catch (e) { /* ignore decode errors */ }
        }
        // Track the most recently heard node for the radar's reticle marker
        if (pkt?.from != null) {
          this.lastHeardNum = pkt.from;
          if (this.tab === "radar") this.drawRadar();
        }
        // Live-refresh node list on telemetry updates (position/nodeinfo handled above)
        if (this.tab === "nodes" && portnum === "TELEMETRY_APP") {
          this.loadNodes();
        }
        // Live-refresh range test log on new range test packets
        if (portnum === "RANGE_TEST_APP" && this.tab === "range") {
          this.loadRangeTest();
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

    setYagiRadio(addr) {
      if (!this.bridgeConfig) return;
      const devices = this.bridgeConfig.devices || {};
      for (const k of Object.keys(devices)) {
        if (devices[k]?.role === 'yagi') delete devices[k];
      }
      if (addr) devices[addr.toUpperCase()] = { role: 'yagi' };
      this.bridgeConfig.devices = devices;
      this.yagiRadioAddr = addr ? addr.toUpperCase() : '';
    },

    // -- Radio Config tab --------------------------------------------------------
    switchCfgTab(name) {
      this.cfgTab = name;
      localStorage.setItem("activeCfgTab", name);
      if (name === "device") this.loadSections();
      else if (name === "bridge") this.loadBridgeConfig();
      else if (name === "rotator") this.loadBridgeConfig();
      else if (name === "channels") this.loadChannels();
      else if (name === "owner") this.loadOwner();
    },

    async loadSections() {
      if (this.allSections.length) return;
      const sec = await fetchJSON("/sections");  // server-level
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
          fetchJSON(`/schema/${sec.name}`),          // server-level
          fetchJSON(this.d(`/config/${sec.name}`)),  // device-level
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
        const res = await fetchJSON(this.d("/fixed_position"));
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
        const res = await fetchJSON(this.d("/fixed_position"), "PUT", body);
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
        const res = await fetchJSON(this.d("/fixed_position"), "DELETE");
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
        const res = await fetchJSON(this.d(`/config/${sec.name}`), "PUT", payload);
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
        const all = await fetchJSON(this.d("/channels"));
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
        if (!this.channelSchema) this.channelSchema = await fetchJSON("/schema/channel");  // server-level
        const live = await fetchJSON(this.d(`/channels/${ch.index}`));
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
        const res = await fetchJSON(this.d(`/channels/${ch.index}`), "PUT", body);
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
      this.ownerSchema = await fetchJSON("/schema/owner");  // server-level
      this.ownerData = await fetchJSON(this.d("/owner"));
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
        const res = await fetchJSON(this.d("/owner"), "PUT", payload);
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
      const text = this.msgText, channel = Number(this.msgChannel), time = new Date().toLocaleTimeString();
      const fromId = this.msgFrom || this.activeNodeId;
      const to = this.msgIsDirect && this.msgDirectTo ? Number(this.msgDirectTo) : 0xFFFFFFFF;
      const body = { text, channel };
      if (this.msgIsDirect && this.msgDirectTo) body.to = to;
      await fetchJSON("/" + fromId + "/messages", "POST", body);
      const fromNum = parseInt(fromId.replace('!', ''), 16) || 0;
      this.messages.unshift({
        fromNum, to: to >>> 0,
        broadcast: to === 0xFFFFFFFF, channel, text, time, direction: 'tx', ackStatus: 'sent',
        src: fromId,
      });
      if (this.messages.length > 50) this.messages.pop();
      this.msgInputHistory = [text, ...this.msgInputHistory.filter(t => t !== text)].slice(0, 50);
      try { localStorage.setItem('msgInputHistory', JSON.stringify(this.msgInputHistory)); } catch (_) {}
      this.msgHistoryIdx = -1;
      this.msgDraft = '';
      this.msgText = "";
      this.msgSent = true;
      setTimeout(() => (this.msgSent = false), 2000);
      if (this.msgIsModal) this.closeMessageModal();
    },

    // -- Reusable message modal (broadcast / direct), callable from any tab ----------
    openMessageModal(mode, node) {
      this.msgIsDirect = mode === 'direct';
      this.msgDirectTo = mode === 'direct' ? (node?.num ?? '') : '';
      this.msgInsertNode = node?.num ?? null;
      this.msgText = '';
      this.msgSent = false;
      this.msgIsModal = true;
      this.$nextTick(() => this.$refs.msgModalDialog?.showModal());
    },

    closeMessageModal() {
      this.msgIsModal = false;
      this.$refs.msgModalDialog?.close();
    },

    replyTo(m) {
      this.msgInsertNode = m.direction === 'tx' ? m.to : m.fromNum;
      if (!m.broadcast) {
        this.msgIsDirect = true;
        this.msgDirectTo = this.msgInsertNode;
      }
    },

    insertText(val) {
      if (!val) return;
      const ta = this._composeTa;
      if (!ta) { this.msgText += val; return; }
      const s = ta.selectionStart, e = ta.selectionEnd;
      this.msgText = this.msgText.slice(0, s) + val + this.msgText.slice(e);
      this.$nextTick(() => { ta.selectionStart = ta.selectionEnd = s + val.length; ta.focus(); });
    },

    insertNodeMeta() {
      if (!this.msgInsertNode) return null;
      const node = this.nodes.find(n => n.num === this.msgInsertNode);
      const lat = node?.position?.latitude_i != null ? node.position.latitude_i / 1e7 : null;
      const lon = node?.position?.longitude_i != null ? node.position.longitude_i / 1e7 : null;
      const hp = this.homePos || (this.fixedPosition.lat != null ? { lat: this.fixedPosition.lat, lon: this.fixedPosition.lon } : null);
      const dist = (hp && lat != null) ? haversine(hp.lat, hp.lon, lat, lon).toFixed(1) : null;
      const az = (hp && lat != null) ? Math.round(bearing(hp.lat, hp.lon, lat, lon)) : null;
      return {
        shortName: this.nodeShortName(this.msgInsertNode),
        longName: this.nodeLongName(this.msgInsertNode),
        dist, az,
      };
    },

    navigateMsgHistory(e, dir) {
      const h = this.msgInputHistory;
      if (!h.length) return;
      const ta = e.target;
      if (dir < 0 && ta.selectionStart > 0) return;
      if (dir > 0 && ta.selectionEnd < ta.value.length) return;
      e.preventDefault();
      if (dir < 0) {
        if (this.msgHistoryIdx === -1) this.msgDraft = this.msgText;
        this.msgHistoryIdx = Math.min(this.msgHistoryIdx + 1, h.length - 1);
      } else {
        if (this.msgHistoryIdx < 0) return;
        this.msgHistoryIdx--;
        if (this.msgHistoryIdx < 0) { this.msgText = this.msgDraft; return; }
      }
      this.msgText = h[this.msgHistoryIdx];
    },

    mentionedNodes() {
      const q = this.mentionQuery.toLowerCase();
      return this.nodes
        .filter(n => n.user?.short_name && (
          n.user.short_name.toLowerCase().startsWith(q) ||
          (n.user.long_name || '').toLowerCase().startsWith(q)
        ))
        .slice(0, 8);
    },

    handleMsgInput(e) {
      this._composeTa = e.target;
      const ta = e.target;
      const before = ta.value.slice(0, ta.selectionStart);
      const m = before.match(/@(\w*)$/);
      if (m) {
        this.mentionQuery = m[1];
        this.mentionPos = before.lastIndexOf('@');
        this.mentionIdx = 0;
        this.mentionOpen = this.mentionedNodes().length > 0;
      } else {
        this.mentionOpen = false;
      }
    },

    handleMsgKeydown(e) {
      this._composeTa = e.target;
      if (this.mentionOpen) {
        const items = this.mentionedNodes();
        if (e.key === 'ArrowDown') { e.preventDefault(); this.mentionIdx = Math.min(this.mentionIdx + 1, items.length - 1); return; }
        if (e.key === 'ArrowUp')   { e.preventDefault(); this.mentionIdx = Math.max(this.mentionIdx - 1, 0); return; }
        if (e.key === 'Enter' || e.key === 'Tab') { e.preventDefault(); if (items[this.mentionIdx]) this.selectMention(items[this.mentionIdx]); return; }
        if (e.key === 'Escape')    { e.preventDefault(); this.mentionOpen = false; return; }
        return;
      }
      if (e.key === 'ArrowUp')   this.navigateMsgHistory(e, -1);
      if (e.key === 'ArrowDown') this.navigateMsgHistory(e, 1);
    },

    selectMention(node) {
      const sn = node.user?.short_name || ('!' + node.num.toString(16).slice(-4));
      const ta = this._composeTa;
      const cursorAfterAt = this.mentionPos + 1 + this.mentionQuery.length;
      this.msgText = this.msgText.slice(0, this.mentionPos) + '@' + sn + ' ' + this.msgText.slice(cursorAfterAt);
      this.mentionOpen = false;
      this.$nextTick(() => { if (ta) { ta.selectionStart = ta.selectionEnd = this.mentionPos + sn.length + 2; ta.focus(); } });
    },

    // -- Yagi rotator "Point" (future integration: see radar-revamp task goal) -------
    playMsgSound() {
      try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const o = ctx.createOscillator(), g = ctx.createGain();
        o.connect(g); g.connect(ctx.destination);
        o.type = 'sine'; o.frequency.value = 880;
        g.gain.setValueAtTime(0.25, ctx.currentTime);
        g.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.35);
        o.start(ctx.currentTime); o.stop(ctx.currentTime + 0.35);
      } catch (_) {}
    },

    nodeShortName(num) {
      const n = this.nodes.find(n => n.num === num);
      return n?.user?.short_name || ('!' + (num & 0xFFFF).toString(16).toUpperCase());
    },
    nodeLongName(num) {
      const n = this.nodes.find(n => n.num === num);
      return n?.user?.long_name || n?.user?.short_name || ('!' + (num & 0xFFFF).toString(16).toUpperCase());
    },
    avatarColor(num) {
      const h = ((num >>> 0) * 2654435761 >>> 0) % 360;
      return `hsl(${h},55%,45%)`;
    },

    deviceLabel(nodeId) {
      if (!nodeId) return '';
      const d = this.availableDevices.find(d => d.node_id === nodeId);
      return d?.short_name || nodeId.replace('!', '').slice(-4).toUpperCase();
    },

    async pointAtNode(node) {
      if (node._az == null) return;
      await fetchJSON(this.d("/rpc"), "POST", { method: "yagi_point", params: { az: node._az, node: node.num, name: node.user?.long_name || "" } });
    },

    // -- Range test tab --------------------------------------------------------------
    async loadRangeTest() {
      this.rangeLoading = true;
      try {
        const data = await fetchJSON(this.d("/range_test"));
        this.rangeLog = (data.log || []).slice().reverse(); // newest first for display
      } catch (e) {
        console.warn("Range test fetch failed", e);
      } finally {
        this.rangeLoading = false;
      }
    },

    async clearRangeTest() {
      await fetchJSON(this.d("/range_test"), "DELETE");
      this.rangeLog = [];
    },

    // Enrich a raw log entry with computed dist/az/link-budget fields.
    rangeEnrich(e) {
      const node = this.nodes.find(n => n.num === e.from_num);
      const lat = node?.position?.latitude_i != null ? node.position.latitude_i / 1e7 : null;
      const lon = node?.position?.longitude_i != null ? node.position.longitude_i / 1e7 : null;
      const distKm = (this.homePos && lat != null)
        ? haversine(this.homePos.lat, this.homePos.lon, lat, lon) : null;
      const az = (this.homePos && lat != null)
        ? bearing(this.homePos.lat, this.homePos.lon, lat, lon) : null;

      const ant = this.bridgeConfig?.antenna;
      let expectedRssi = null, excessLoss = null;
      if (distKm != null && distKm > 0 && ant) {
        const fspl = 20 * Math.log10(distKm) + 20 * Math.log10(868) + 32.4;
        const txPow  = ant.remote_default?.tx_power_dbm ?? 22;
        const txGain = ant.remote_default?.gain_dbi ?? 2;
        const rxNet  = (ant.rx?.gain_dbi ?? 9.5) - (ant.rx?.cable_loss_db ?? 0);
        expectedRssi = Math.round(txPow + txGain + rxNet - fspl);
        if (e.rssi != null) excessLoss = parseFloat((expectedRssi - e.rssi).toFixed(1));
      }

      return {
        ...e,
        nodeName: node?.user?.long_name || node?.user?.short_name
          || ("!" + (e.from_num ?? 0).toString(16).slice(-4)),
        distKm,
        az: az != null ? Math.round(az) : null,
        expectedRssi,
        excessLoss,
      };
    },

    rangeStats() {
      const enriched = this.rangeLog.map(e => this.rangeEnrich(e)).filter(e => e.rssi != null);
      if (!enriched.length) return null;
      const rssis  = enriched.map(e => e.rssi).sort((a, b) => a - b);
      const losses = enriched.filter(e => e.excessLoss != null).map(e => e.excessLoss).sort((a, b) => a - b);
      const dists  = enriched.filter(e => e.distKm != null).map(e => e.distKm);
      const median = arr => arr[Math.floor(arr.length / 2)];
      return {
        count:          enriched.length,
        medianRssi:     median(rssis),
        bestRssi:       rssis[rssis.length - 1],
        worstRssi:      rssis[0],
        medianExcessLoss: losses.length ? parseFloat(median(losses).toFixed(1)) : null,
        maxDistKm:      dists.length ? Math.max(...dists).toFixed(1) : null,
      };
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
      if (!this.homePos) return;
      this.refreshRadar();
      this.geocodeNodes();
    },

    refreshRadar() {
      if (!this.homePos) return;
      this.radarNodes = this.filteredNodes()
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
      if (!this.homePos) return;
      const maxKm = this.radarRange === "0"
        ? (this.radarNodes.length ? Math.max(...this.radarNodes.map((n) => n._km)) * 1.15 : 50)
        : Number(this.radarRange);
      this._drawRadarBg(maxKm);
      this._drawRadarBeam();
      this._drawRadarNodes(maxKm);
    },

    _radarNorm(km, maxKm) {
      if (!km || !maxKm) return 0;
      const f = this.radarLogScale
        ? Math.log1p(km) / Math.log1p(maxKm)
        : km / maxKm;
      return Math.min(f, 1.0);
    },

    _drawRadarBg(maxKm) {
      const bg = document.getElementById('radar-bg-g');
      if (!bg) return;
      const CX = 300, CY = 300, R = 256;
      const G0 = "rgba(0,255,80,0.06)";
      const G1 = "rgba(0,255,80,0.45)";
      const G2 = "rgba(0,255,80,0.40)";
      const G3 = "rgba(0,255,80,0.70)";
      const G4 = "rgba(0,255,80,0.95)";
      bg.innerHTML = '';
      bg.appendChild(svgElem('circle', { cx: CX, cy: CY, r: R, style: 'fill:url(#radarBg)' }));
      const scanG = svgElem('g', { 'clip-path': 'url(#radarClip)', style: 'pointer-events:none' });
      for (let yy = CY - R; yy < CY + R; yy += 4)
        scanG.appendChild(svgElem('line', { x1: CX - R, y1: yy, x2: CX + R, y2: yy, style: 'stroke:rgba(0,0,0,0.10);stroke-width:1' }));
      bg.appendChild(scanG);
      const ringKms = this.radarLogScale
        ? (() => {
            // Pick up to 3 nice round values inside maxKm, plus maxKm itself
            const cands = [1, 2, 5, 10, 20, 50, 100, 150, 250, 500, 1000].filter(k => k < maxKm);
            return [...cands.slice(-3), maxKm];
          })()
        : [1, 2, 3, 4].map(i => maxKm * i / 4);
      ringKms.forEach((km, idx) => {
        const isFull = idx === ringKms.length - 1;
        const r = this._radarNorm(km, maxKm) * R;
        bg.appendChild(svgElem('circle', { cx: CX, cy: CY, r,
          style: `fill:none;stroke:${isFull ? G2 : G1};stroke-width:${isFull ? 1.2 : 0.9};stroke-dasharray:${isFull ? '' : '5 5'}` }));
        const lbl = svgElem('text', { x: CX + 5, y: CY - r + 12,
          style: `fill:${G3};font-size:10px;font-family:'Oxanium',monospace;letter-spacing:0.05em` });
        lbl.textContent = (km < 10 ? km.toFixed(km % 1 ? 1 : 0) : km) + ' km';
        bg.appendChild(lbl);
      });
      if (this.radarCrosshair) {
        bg.appendChild(svgElem('line', { x1: CX, y1: CY - R, x2: CX, y2: CY + R, style: `stroke:${G1};stroke-width:0.7;stroke-dasharray:2 8` }));
        bg.appendChild(svgElem('line', { x1: CX - R, y1: CY, x2: CX + R, y2: CY, style: `stroke:${G1};stroke-width:0.7;stroke-dasharray:2 8` }));
        const d45 = R * 0.707;
        bg.appendChild(svgElem('line', { x1: CX - d45, y1: CY - d45, x2: CX + d45, y2: CY + d45, style: `stroke:${G0};stroke-width:0.6;stroke-dasharray:2 10` }));
        bg.appendChild(svgElem('line', { x1: CX + d45, y1: CY - d45, x2: CX - d45, y2: CY + d45, style: `stroke:${G0};stroke-width:0.6;stroke-dasharray:2 10` }));
      }
      for (let deg = 0; deg < 360; deg += 10) {
        const isMajor = deg % 30 === 0, tickLen = isMajor ? 11 : 5;
        const rad = deg * Math.PI / 180;
        const ox = CX + Math.sin(rad) * R, oy = CY - Math.cos(rad) * R;
        const ix = CX + Math.sin(rad) * (R - tickLen), iy = CY - Math.cos(rad) * (R - tickLen);
        bg.appendChild(svgElem('line', { x1: ox, y1: oy, x2: ix, y2: iy,
          style: `stroke:rgba(0,255,80,${isMajor ? 0.70 : 0.38});stroke-width:${isMajor ? 1.2 : 0.8}` }));
      }
      bg.appendChild(svgElem('circle', { cx: CX, cy: CY, r: R, style: `fill:none;stroke:${G2};stroke-width:1.5;filter:url(#rimGlow)` }));
      bg.appendChild(svgElem('circle', { cx: CX, cy: CY, r: R + 6, style: 'fill:none;stroke:rgba(0,255,80,0.10);stroke-width:3' }));
      bg.appendChild(svgElem('circle', { cx: CX, cy: CY, r: R + 10, style: 'fill:none;stroke:rgba(0,255,80,0.04);stroke-width:2' }));
      for (const [label, dx, dy] of [['N', 0, -1], ['E', 1, 0], ['S', 0, 1], ['W', -1, 0]]) {
        const t = svgElem('text', { x: CX + dx * (R + 22), y: CY + dy * (R + 22) + 5,
          style: `fill:${G4};font-size:13px;font-weight:700;font-family:'Oxanium',monospace;text-anchor:middle;letter-spacing:0.1em` });
        t.textContent = label;
        bg.appendChild(t);
      }
    },

    _drawRadarBeam() {
      const beamG = document.getElementById('radar-beam-g');
      if (!beamG || this.yagiAz == null) return;
      const CX = 300, CY = 300, R = 256;
      const bw = this.bridgeConfig?.rotator?.beam_width_deg ?? 5;
      const HW = (bw / 2) * Math.PI / 180;
      const wx1 = CX + Math.sin(-HW) * R, wy1 = CY - Math.cos(-HW) * R;
      const wx2 = CX + Math.sin(HW) * R,  wy2 = CY - Math.cos(HW) * R;
      const az = Math.round(this._radarBeamAz ?? this.yagiAz);
      // innerHTML replaces only children — group's own style.transition and style.transform are unchanged
      beamG.innerHTML = `<path d="M ${CX} ${CY} L ${wx1.toFixed(1)} ${wy1.toFixed(1)} A ${R} ${R} 0 0 1 ${wx2.toFixed(1)} ${wy2.toFixed(1)} Z" style="fill:rgba(80,200,255,0.14);stroke:none;clip-path:url(#radarClip)"/><line x1="${CX}" y1="${CY}" x2="${CX}" y2="${CY - R}" style="stroke:rgba(80,200,255,0.85);stroke-width:2;opacity:0.9"/><text x="${CX}" y="${CY - R - 15}" style="fill:rgba(255,50,50,0.95);font-size:11px;font-weight:700;font-family:'Oxanium',monospace;text-anchor:middle;dominant-baseline:middle;pointer-events:none">${az}°</text>`;
    },

    _drawRadarNodes(maxKm) {
      const ng = document.getElementById('radar-nodes-g');
      if (!ng) return;
      ng.innerHTML = '';
      const nodes = this.radarNodes;
      const CX = 300, CY = 300, R = 256;
      const G4 = 'rgba(0,255,80,0.95)';
      const AMBER = 'rgba(255,200,40,0.90)';
      const CLUSTER_R = 22, BASE_DIAG = 12, STEP_DIAG = 14, HOR_LEN = 16;
      const selectedNum = this.radarSelected?.num;
      const lastHeardNum = this.lastHeardNum;
      const pointTarget = this.yagiPointTarget;

      const npos = nodes.map(node => {
        const az = node._az * Math.PI / 180;
        const normKm = this._radarNorm(node._km, maxKm);
        return { x: CX + Math.sin(az) * normKm * R, y: CY - Math.cos(az) * normKm * R, diagLen: BASE_DIAG, isRight: null };
      });
      const clusterOf = new Array(npos.length).fill(-1);
      for (let i = 0; i < npos.length; i++) {
        if (clusterOf[i] >= 0) continue;
        const members = [i]; clusterOf[i] = i;
        for (let j = i + 1; j < npos.length; j++) {
          if (clusterOf[j] >= 0) continue;
          const dx = npos[i].x - npos[j].x, dy = npos[i].y - npos[j].y;
          if (dx * dx + dy * dy < CLUSTER_R * CLUSTER_R) { members.push(j); clusterOf[j] = i; }
        }
        if (members.length > 1) members.forEach((idx, rank) => { npos[idx].diagLen = BASE_DIAG + rank * STEP_DIAG; npos[idx].isRight = rank % 2 === 0; });
      }

      nodes.forEach((node, ni) => {
        const { x, y, diagLen } = npos[ni];
        const isRight = npos[ni].isRight !== null ? npos[ni].isRight : x >= CX;
        const dotColor = ageColor(node.last_heard, this.heatmapMaxAge);
        const isSelected = node.num === selectedNum;
        const isLastHeard = node.num === lastHeardNum;
        const g = svgElem('g', { class: 'radar-node' + (isSelected ? ' radar-node-selected' : ''), style: 'cursor:pointer' });
        if (isLastHeard) {
          const rs = `stroke:${AMBER};stroke-width:1.2`;
          g.appendChild(svgElem('circle', { cx: x, cy: y, r: 13, style: `fill:none;${rs};stroke-dasharray:3 4` }));
          g.appendChild(svgElem('line', { x1: x-17, y1: y, x2: x-7, y2: y, style: rs }));
          g.appendChild(svgElem('line', { x1: x+7, y1: y, x2: x+17, y2: y, style: rs }));
          g.appendChild(svgElem('line', { x1: x, y1: y-17, x2: x, y2: y-7, style: rs }));
          g.appendChild(svgElem('line', { x1: x, y1: y+7, x2: x, y2: y+17, style: rs }));
        }
        if (isSelected)
          g.appendChild(svgElem('circle', { cx: x, cy: y, r: 12, style: `fill:none;stroke:${G4};stroke-width:1.5;stroke-dasharray:4 3` }));
        g.appendChild(svgElem('circle', { cx: x, cy: y, r: isSelected ? 5 : 4, style: `fill:${dotColor};filter:url(#blipGlow)` }));
        const title = svgElem('title');
        title.textContent = node.user?.long_name || node.user?.id || ('!' + (node.num ?? 0).toString(16).slice(-4));
        g.appendChild(title);
        const label = node.user?.short_name || ('!' + (node.num ?? 0).toString(16).slice(-4));
        const diagSign = isRight ? 1 : -1;
        const elbowX = x + diagSign * diagLen, elbowY = y - diagLen;
        const capX = elbowX + diagSign * HOR_LEN, capY = elbowY;
        g.appendChild(svgElem('line', { x1: x + diagSign * 5, y1: y - 4, x2: elbowX, y2: elbowY, style: `stroke:${dotColor};stroke-width:0.8;opacity:0.7;pointer-events:none` }));
        g.appendChild(svgElem('line', { x1: elbowX, y1: elbowY, x2: capX, y2: capY, style: `stroke:${dotColor};stroke-width:0.8;opacity:0.7;pointer-events:none` }));
        const txt = svgElem('text', { class: 'radar-node-label', x: capX + diagSign * 3, y: capY + 4,
          style: `fill:${dotColor};font-size:10px;font-family:'Oxanium',monospace;pointer-events:none;text-anchor:${isRight ? 'start' : 'end'}` });
        txt.textContent = label;
        g.appendChild(txt);
        g.addEventListener('click', (e) => { e.stopPropagation(); this.radarSelected = node; this.openNodeInfo(node); });
        ng.appendChild(g);
      });

      const hSize = 7;
      ng.appendChild(svgElem('polygon', { points: `${CX},${CY-hSize} ${CX+hSize},${CY} ${CX},${CY+hSize} ${CX-hSize},${CY}`, style: `fill:${G4};filter:url(#blipGlow)` }));
      ng.appendChild(svgElem('line', { x1: CX-16, y1: CY, x2: CX-hSize-1, y2: CY, style: `stroke:${G4};stroke-width:1.2` }));
      ng.appendChild(svgElem('line', { x1: CX+hSize+1, y1: CY, x2: CX+16, y2: CY, style: `stroke:${G4};stroke-width:1.2` }));
      ng.appendChild(svgElem('line', { x1: CX, y1: CY-16, x2: CX, y2: CY-hSize-1, style: `stroke:${G4};stroke-width:1.2` }));
      ng.appendChild(svgElem('line', { x1: CX, y1: CY+hSize+1, x2: CX, y2: CY+16, style: `stroke:${G4};stroke-width:1.2` }));

      // Crosshairs drawn last so they're always on top of all node blips
      if (pointTarget) {
        const ti = nodes.findIndex(n => n.num === pointTarget);
        if (ti >= 0) {
          const { x, y } = npos[ti];
          const rs = 'stroke:rgba(255,30,30,0.95);stroke-width:1.8';
          const cg = svgElem('g');
          cg.appendChild(svgElem('circle', { cx: x, cy: y, r: 16, style: `fill:none;${rs};stroke-dasharray:4 3` }));
          cg.appendChild(svgElem('line', { x1: x-22, y1: y, x2: x-10, y2: y, style: rs }));
          cg.appendChild(svgElem('line', { x1: x+10, y1: y, x2: x+22, y2: y, style: rs }));
          cg.appendChild(svgElem('line', { x1: x, y1: y-22, x2: x, y2: y-10, style: rs }));
          cg.appendChild(svgElem('line', { x1: x, y1: y+10, x2: x, y2: y+22, style: rs }));
          ng.appendChild(cg);
        }
      }
    },

    // -- Reusable Node Info modal, callable from Radar and Nodes table ----------------
    openNodeInfo(node) {
      const radarNode = this.radarNodes.find((r) => r.num === node.num);
      this.nodeInfo = radarNode || { ...node, _km: this.nodeKm(node), _az: this.nodeAz(node) };
      this.$nextTick(() => this.$refs.nodeInfoDialog?.showModal());
      if (!this.nodeInfo._address && this.nodeInfo._lat != null && this.nodeInfo._lon != null) {
        geocodeLatLon(this.nodeInfo._lat, this.nodeInfo._lon).then((addr) => {
          if (this.nodeInfo?.num === node.num) this.nodeInfo = { ...this.nodeInfo, _address: addr };
        });
      }
    },

    toggleRadarCrosshair() {
      this.radarCrosshair = !this.radarCrosshair;
      this.saveRadarPref("crosshair_default", this.radarCrosshair);
      this.drawRadar();
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
  const base = window.MESH_API || "";
  const res = await fetch(base + url, opts);
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

// Map LoRa RSSI (dBm) to a rough signal-strength percentage for display.
function rssiPercent(rssi) {
  if (rssi == null) return null;
  return Math.max(0, Math.min(100, Math.round((rssi + 120) / 70 * 100)));
}

// Age-based colour: fresh -> bright amber/white, stale -> orange-red.
// Deliberately avoids green (hue ~120) so blips never blend into the radar grid.
function ageColor(lastHeard, maxAge = 3600) {
  if (!lastHeard) return "rgba(255,255,255,0.25)";
  const ageSec = Math.max(0, Date.now() / 1000 - lastHeard);
  const t = Math.min(ageSec / maxAge, 1);
  const hue = 55 - t * 55;        // 55=warm amber (fresh) -> 0=red (stale)
  const lit  = 82 - t * 32;       // 82% bright fresh -> 50% dimmer stale
  return `hsl(${hue}, 92%, ${lit}%)`;
}

function svgElem(tag, attrs = {}) {
  const el = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "style") { el.style.cssText = v; }
    else el.setAttribute(k, v);
  }
  return el;
}

function buildRadarSVG(nodes, maxKm, opts = {}) {
  const { crosshair = true, heatmapMaxAge = 3600, selectedNum = null, lastHeardNum = null, pointTarget = null, yagiAz = null, beamWidth = 5, onSelect = () => {} } = opts;
  const SIZE = 600;
  const CX = SIZE / 2, CY = SIZE / 2, R = SIZE / 2 - 44;

  // Radar always renders dark — real radar scopes don't switch to light mode
  const G0  = "rgba(0,255,80,0.06)";   // almost invisible fill tint
  const G1  = "rgba(0,255,80,0.45)";   // range rings
  const G2  = "rgba(0,255,80,0.40)";   // medium labels
  const G3  = "rgba(0,255,80,0.70)";   // compass, range text
  const G4  = "rgba(0,255,80,0.95)";   // bright accents / home
  const AMBER = "rgba(255,200,40,0.90)"; // reticle / last-heard

  const svg = svgElem("svg", {
    viewBox: `0 0 ${SIZE} ${SIZE}`,
    width: "100%",
    // No CSS background — container is already dark; SVG background is an SVG circle element
    style: "display:block;margin:auto;overflow:visible;max-height:100%",
  });

  const defs = svgElem("defs");

  // Radial gradient: slightly brighter centre fading to near-black edge
  const radGrad = svgElem("radialGradient", { id: "radarBg", cx: "50%", cy: "50%", r: "50%" });
  for (const [off, col] of [["0%","rgba(0,30,8,1)"],["60%","rgba(0,15,4,1)"],["100%","rgba(0,5,2,1)"]]) {
    const s = svgElem("stop"); s.setAttribute("offset", off); s.setAttribute("stop-color", col); radGrad.appendChild(s);
  }
  defs.appendChild(radGrad);

  // Outer glow filter for the rim
  const rimGlow = svgElem("filter", { id: "rimGlow", x: "-10%", y: "-10%", width: "120%", height: "120%" });
  const gb = svgElem("feGaussianBlur", { stdDeviation: "3", result: "blur" });
  const fm = svgElem("feMerge");
  [svgElem("feMergeNode"), (() => { const n = svgElem("feMergeNode"); n.setAttribute("in","SourceGraphic"); return n; })()].forEach(n => fm.appendChild(n));
  rimGlow.appendChild(gb); rimGlow.appendChild(fm);
  defs.appendChild(rimGlow);

  // Blip glow filter for node dots
  const blipGlow = svgElem("filter", { id: "blipGlow", x: "-80%", y: "-80%", width: "260%", height: "260%" });
  const blipBlur = svgElem("feGaussianBlur", { stdDeviation: "2.5", result: "blur" });
  const blipMerge = svgElem("feMerge");
  [svgElem("feMergeNode"), (() => { const n = svgElem("feMergeNode"); n.setAttribute("in","SourceGraphic"); return n; })()].forEach(n => blipMerge.appendChild(n));
  blipGlow.appendChild(blipBlur); blipGlow.appendChild(blipMerge);
  defs.appendChild(blipGlow);

  // Beam glow filter for Yagi beam line
  const beamGlow = svgElem("filter", { id: "beamGlow", x: "-20%", y: "-20%", width: "140%", height: "140%" });
  const beamBlur = svgElem("feGaussianBlur", { stdDeviation: "4", result: "blur" });
  const beamMerge = svgElem("feMerge");
  [svgElem("feMergeNode"), (() => { const n = svgElem("feMergeNode"); n.setAttribute("in","SourceGraphic"); return n; })()].forEach(n => beamMerge.appendChild(n));
  beamGlow.appendChild(beamBlur); beamGlow.appendChild(beamMerge);
  defs.appendChild(beamGlow);

  // Clip to the radar circle
  const clip = svgElem("clipPath", { id: "radarClip" });
  clip.appendChild(svgElem("circle", { cx: CX, cy: CY, r: R }));
  defs.appendChild(clip);

  svg.appendChild(defs);

  // Background disc
  svg.appendChild(svgElem("circle", { cx: CX, cy: CY, r: R, style: "fill:url(#radarBg)" }));

  // Subtle scan-line texture (horizontal lines every 4px clipped to disc)
  const scanG = svgElem("g", { "clip-path": "url(#radarClip)", style: "pointer-events:none" });
  for (let yy = CY - R; yy < CY + R; yy += 4) {
    scanG.appendChild(svgElem("line", { x1: CX - R, y1: yy, x2: CX + R, y2: yy,
      style: "stroke:rgba(0,0,0,0.10);stroke-width:1" }));
  }
  svg.appendChild(scanG);

  // Concentric range rings
  for (let i = 1; i <= 4; i++) {
    const r = R * i / 4;
    const isFull = (i === 4);
    svg.appendChild(svgElem("circle", { cx: CX, cy: CY, r,
      style: `fill:none;stroke:${isFull ? G2 : G1};stroke-width:${isFull ? 1.2 : 0.9};stroke-dasharray:${isFull ? "" : "5 5"}` }));
    const lbl = svgElem("text", { x: CX + 5, y: CY - r + 12,
      style: `fill:${G3};font-size:10px;font-family:'Oxanium',monospace;letter-spacing:0.05em` });
    lbl.textContent = (maxKm * i / 4 | 0) + " km";
    svg.appendChild(lbl);
  }

  // Crosshair lines (N-S and E-W)
  if (crosshair) {
    svg.appendChild(svgElem("line", { x1: CX, y1: CY - R, x2: CX, y2: CY + R,
      style: `stroke:${G1};stroke-width:0.7;stroke-dasharray:2 8` }));
    svg.appendChild(svgElem("line", { x1: CX - R, y1: CY, x2: CX + R, y2: CY,
      style: `stroke:${G1};stroke-width:0.7;stroke-dasharray:2 8` }));
    // Diagonal faint lines at 45°
    const d45 = R * 0.707;
    svg.appendChild(svgElem("line", { x1: CX - d45, y1: CY - d45, x2: CX + d45, y2: CY + d45,
      style: `stroke:${G0};stroke-width:0.6;stroke-dasharray:2 10` }));
    svg.appendChild(svgElem("line", { x1: CX + d45, y1: CY - d45, x2: CX - d45, y2: CY + d45,
      style: `stroke:${G0};stroke-width:0.6;stroke-dasharray:2 10` }));
  }

  // Degree tick marks — radial lines inward from the rim (major every 30°, minor every 10°)
  for (let deg = 0; deg < 360; deg += 10) {
    const isMajor = (deg % 30 === 0);
    const tickLen = isMajor ? 11 : 5;
    const rad = deg * Math.PI / 180;
    const ox = CX + Math.sin(rad) * R;
    const oy = CY - Math.cos(rad) * R;
    const ix = CX + Math.sin(rad) * (R - tickLen);
    const iy = CY - Math.cos(rad) * (R - tickLen);
    svg.appendChild(svgElem("line", { x1: ox, y1: oy, x2: ix, y2: iy,
      style: `stroke:rgba(0,255,80,${isMajor ? 0.70 : 0.38});stroke-width:${isMajor ? 1.2 : 0.8}` }));
  }

  // Rim — bright outer border with glow
  svg.appendChild(svgElem("circle", { cx: CX, cy: CY, r: R,
    style: `fill:none;stroke:${G2};stroke-width:1.5;filter:url(#rimGlow)` }));
  // Subtle secondary ring just outside — gives depth to the bezel
  svg.appendChild(svgElem("circle", { cx: CX, cy: CY, r: R + 6,
    style: `fill:none;stroke:rgba(0,255,80,0.10);stroke-width:3` }));
  svg.appendChild(svgElem("circle", { cx: CX, cy: CY, r: R + 10,
    style: `fill:none;stroke:rgba(0,255,80,0.04);stroke-width:2` }));

  // Compass labels (outside the rim, above the bezel rings)
  for (const [label, dx, dy] of [["N", 0, -1], ["E", 1, 0], ["S", 0, 1], ["W", -1, 0]]) {
    const t = svgElem("text", {
      x: CX + dx * (R + 22), y: CY + dy * (R + 22) + 5,
      style: `fill:${G4};font-size:13px;font-weight:700;font-family:'Oxanium',monospace;text-anchor:middle;letter-spacing:0.1em`,
    });
    t.textContent = label;
    svg.appendChild(t);
  }

  // Yagi beam — drawn pointing NORTH (0°); CSS transform rotates it with transition
  // The caller (drawRadar) animates the transform after appending to DOM.
  if (yagiAz != null) {
    const BEAM_COLOR = "rgba(80,200,255,0.85)";
    const WEDGE_COLOR = "rgba(80,200,255,0.14)";
    const HW = (beamWidth / 2) * Math.PI / 180;
    // Wedge edges at ±HW from north
    const wx1 = CX + Math.sin(-HW) * R;
    const wy1 = CY - Math.cos(-HW) * R;
    const wx2 = CX + Math.sin(HW) * R;
    const wy2 = CY - Math.cos(HW) * R;
    const beamG = svgElem("g", {
      class: "radar-beam",
      style: `transform-box:view-box;transform-origin:${CX}px ${CY}px`,
    });
    beamG.appendChild(svgElem("path", {
      d: `M ${CX} ${CY} L ${wx1.toFixed(1)} ${wy1.toFixed(1)} A ${R} ${R} 0 0 1 ${wx2.toFixed(1)} ${wy2.toFixed(1)} Z`,
      style: `fill:${WEDGE_COLOR};stroke:none;clip-path:url(#radarClip)`,
    }));
    beamG.appendChild(svgElem("line", { x1: CX, y1: CY, x2: CX, y2: CY - R,
      style: `stroke:${BEAM_COLOR};stroke-width:2;opacity:0.9` }));
    // Bearing label at the tip — rotates with the beam
    const bearLbl = svgElem("text", { x: CX, y: CY - (R + 15),
      style: `fill:rgba(255,50,50,0.95);font-size:11px;font-weight:700;font-family:'Oxanium',monospace;text-anchor:middle;dominant-baseline:middle;pointer-events:none` });
    bearLbl.textContent = Math.round(yagiAz) + "°";
    beamG.appendChild(bearLbl);
    svg.appendChild(beamG);
  }

  // Node blips — pass 1: compute screen positions and resolve label collisions
  const CLUSTER_R = 22;   // px — nodes within this radius share a cluster
  const BASE_DIAG = 12;   // base diagonal leader length
  const STEP_DIAG = 14;   // extra length per rank within a cluster
  const HOR_LEN   = 16;   // horizontal shoulder length

  const npos = nodes.map(node => {
    const az = node._az * Math.PI / 180;
    const normKm = Math.min(node._km / maxKm, 1.0);
    return {
      x: CX + Math.sin(az) * normKm * R,
      y: CY - Math.cos(az) * normKm * R,
      diagLen: BASE_DIAG,
      isRight: null,   // null = auto from x position
    };
  });

  // Cluster nearby blips and stagger their leader lengths + alternate direction
  const clusterOf = new Array(npos.length).fill(-1);
  for (let i = 0; i < npos.length; i++) {
    if (clusterOf[i] >= 0) continue;
    const members = [i];
    clusterOf[i] = i;
    for (let j = i + 1; j < npos.length; j++) {
      if (clusterOf[j] >= 0) continue;
      const dx = npos[i].x - npos[j].x;
      const dy = npos[i].y - npos[j].y;
      if (dx * dx + dy * dy < CLUSTER_R * CLUSTER_R) {
        members.push(j);
        clusterOf[j] = i;
      }
    }
    if (members.length > 1) {
      members.forEach((idx, rank) => {
        npos[idx].diagLen = BASE_DIAG + rank * STEP_DIAG;
        npos[idx].isRight = (rank % 2 === 0); // alternate left/right within cluster
      });
    }
  }

  // Pass 2: draw each node
  nodes.forEach((node, ni) => {
    const { x, y, diagLen } = npos[ni];
    const isRight = npos[ni].isRight !== null ? npos[ni].isRight : x >= CX;

    const dotColor = ageColor(node.last_heard, heatmapMaxAge);
    const isSelected = node.num === selectedNum;
    const isLastHeard = node.num === lastHeardNum;
    const isPointTarget = node.num === pointTarget;

    const g = svgElem("g", { class: "radar-node" + (isSelected ? " radar-node-selected" : ""), style: "cursor:pointer" });

    // Red targeting crosshairs — rotator is aimed here (active mode point target)
    if (isPointTarget) {
      const RED = "rgba(255,30,30,0.95)";
      const rs = `stroke:${RED};stroke-width:1.8`;
      g.appendChild(svgElem("circle", { cx: x, cy: y, r: 16, style: `fill:none;${rs};stroke-dasharray:4 3` }));
      g.appendChild(svgElem("line", { x1: x-22, y1: y, x2: x-10, y2: y, style: rs }));
      g.appendChild(svgElem("line", { x1: x+10, y1: y, x2: x+22, y2: y, style: rs }));
      g.appendChild(svgElem("line", { x1: x, y1: y-22, x2: x, y2: y-10, style: rs }));
      g.appendChild(svgElem("line", { x1: x, y1: y+10, x2: x, y2: y+22, style: rs }));
    }

    // Amber reticle on most-recently-heard node
    if (isLastHeard) {
      const rs = "stroke:" + AMBER + ";stroke-width:1.2";
      g.appendChild(svgElem("circle", { cx: x, cy: y, r: 13, style: `fill:none;${rs};stroke-dasharray:3 4` }));
      g.appendChild(svgElem("line", { x1: x-17, y1: y, x2: x-7, y2: y, style: rs }));
      g.appendChild(svgElem("line", { x1: x+7, y1: y, x2: x+17, y2: y, style: rs }));
      g.appendChild(svgElem("line", { x1: x, y1: y-17, x2: x, y2: y-7, style: rs }));
      g.appendChild(svgElem("line", { x1: x, y1: y+7, x2: x, y2: y+17, style: rs }));
    }

    // Selection ring
    if (isSelected) {
      g.appendChild(svgElem("circle", { cx: x, cy: y, r: 12,
        style: `fill:none;stroke:${G4};stroke-width:1.5;stroke-dasharray:4 3` }));
    }

    // Main blip dot with phosphor glow
    g.appendChild(svgElem("circle", { cx: x, cy: y, r: isSelected ? 5 : 4,
      style: `fill:${dotColor};filter:url(#blipGlow)` }));

    const title = svgElem("title");
    title.textContent = node.user?.long_name || node.user?.id || ("!" + (node.num ?? 0).toString(16).slice(-4));
    g.appendChild(title);

    // Leader line with collision-resolved diagLen
    const label = node.user?.short_name || ("!" + (node.num ?? 0).toString(16).slice(-4));
    const diagSign = isRight ? 1 : -1;
    const elbowX = x + diagSign * diagLen;
    const elbowY = y - diagLen;
    const capX = elbowX + diagSign * HOR_LEN;
    const capY = elbowY;
    const startX = x + diagSign * 5;
    const startY = y - 4;
    g.appendChild(svgElem("line", { x1: startX, y1: startY, x2: elbowX, y2: elbowY,
      style: `stroke:${dotColor};stroke-width:0.8;opacity:0.7;pointer-events:none` }));
    g.appendChild(svgElem("line", { x1: elbowX, y1: elbowY, x2: capX, y2: capY,
      style: `stroke:${dotColor};stroke-width:0.8;opacity:0.7;pointer-events:none` }));
    const txt = svgElem("text", {
      class: "radar-node-label",
      x: capX + diagSign * 3, y: capY + 4,
      style: `fill:${dotColor};font-size:10px;font-family:'Oxanium',monospace;pointer-events:none;text-anchor:${isRight ? "start" : "end"}`,
    });
    txt.textContent = label;
    g.appendChild(txt);

    g.addEventListener("click", (e) => { e.stopPropagation(); onSelect(node); });
    svg.appendChild(g);
  });

  // Home position marker — diamond + crosshair in bright green
  const hSize = 7;
  svg.appendChild(svgElem("polygon", {
    points: `${CX},${CY-hSize} ${CX+hSize},${CY} ${CX},${CY+hSize} ${CX-hSize},${CY}`,
    style: `fill:${G4};filter:url(#blipGlow)`,
  }));
  svg.appendChild(svgElem("line", { x1: CX-16, y1: CY, x2: CX-hSize-1, y2: CY, style: `stroke:${G4};stroke-width:1.2` }));
  svg.appendChild(svgElem("line", { x1: CX+hSize+1, y1: CY, x2: CX+16, y2: CY, style: `stroke:${G4};stroke-width:1.2` }));
  svg.appendChild(svgElem("line", { x1: CX, y1: CY-16, x2: CX, y2: CY-hSize-1, style: `stroke:${G4};stroke-width:1.2` }));
  svg.appendChild(svgElem("line", { x1: CX, y1: CY+hSize+1, x2: CX, y2: CY+16, style: `stroke:${G4};stroke-width:1.2` }));

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
