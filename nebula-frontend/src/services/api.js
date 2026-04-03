const BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';
const protocol = BASE_URL.startsWith('https') ? 'wss://' : 'ws://';
const domainAndPath = BASE_URL.replace(/^https?:\/\//, '');
const WS_URL = `${protocol}${domainAndPath}/ws/metrics`;

const USE_MOCK = false; // Toggled to false to hit real backend

// Fallback Mock Data
let MOCK_NODES = Array.from({ length: 4 }).map((_, i) => ({
  id: `node_mock_${i + 1}`,
  cpu: Math.floor(Math.random() * 32) + 4,
  ram: Math.floor(Math.random() * 64) + 8,
  trust: Math.floor(Math.random() * 50) + 50,
  status: Math.random() > 0.2 ? 'online' : (Math.random() > 0.5 ? 'busy' : 'offline'),
  credits: Math.floor(Math.random() * 1000)
}));

export const api = {
  getNodes: async () => {
    try {
      if (USE_MOCK) throw new Error("Forced mock");
      const res = await fetch(`${BASE_URL}/get_nodes`);
      if (!res.ok) throw new Error("Failed to fetch nodes");
      return await res.json();
    } catch (e) {
      console.warn("Using mock nodes due to error:", e);
      return MOCK_NODES;
    }
  },
  
  getClusterStatus: async () => {
    try {
      if (USE_MOCK) throw new Error("Forced mock");
      const res = await fetch(`${BASE_URL}/status`);
      if (!res.ok) throw new Error("Failed to fetch status");
      return await res.json();
    } catch (e) {
      console.warn("Using mock status due to error:", e);
      return {
        cluster: { total_nodes: 4, online_nodes: 3, offline_nodes: 1 },
        nodes: MOCK_NODES,
        recent_jobs: [],
        pipeline_tasks: []
      };
    }
  },
  
  submitJob: async (type = 'mnist') => {
    try {
      if (USE_MOCK) return { success: true };
      const endpoint = type === 'mnist' ? '/demo_mnist' 
                     : type === 'fashion' ? '/demo_fashion' 
                     : '/demo_cifar';
      const res = await fetch(`${BASE_URL}${endpoint}`, { method: 'POST' });
      return await res.json();
    } catch (e) {
      console.error("Job submission failed:", e);
      return { success: false };
    }
  },

  simulateNodeFailure: async (nodeId) => {
    try {
      if (USE_MOCK) return { success: true };
      const res = await fetch(`${BASE_URL}/simulate_failure/${nodeId}`, { method: 'POST' });
      return await res.json();
    } catch (e) {
      console.error("Simulation failed:", e);
      return { success: false };
    }
  },

  simulateNodeJoin: async () => {
    try {
      if (USE_MOCK) return { success: true };
      const newNode = {
        id: `node_web_${Math.floor(Math.random() * 10000)}`,
        cpu: 16,
        ram: 32,
        gpu: false,
        hostname: "web-contributor",
        capabilities: ["training"]
      };
      
      const res = await fetch(`${BASE_URL}/register_node`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(newNode)
      });
      return await res.json();
    } catch (e) {
       console.error("Node join failed:", e);
       return { success: false };
    }
  }
};

// WebSocket implementation with reconnect
class WebSocketService {
  constructor() {
    this.listeners = {};
    this.ws = null;
    this.reconnectAttempts = 0;
    this.shouldReconnect = true;
    this.connect();
  }

  connect() {
    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
        return;
    }
    
    try {
        console.log(`📡 Connecting to ${WS_URL}...`);
        this.ws = new WebSocket(WS_URL);

        this.ws.onopen = () => {
            console.log("✅ WebSocket Connected!");
            this.reconnectAttempts = 0;
            this.emit('connection', { status: 'connected' });
        };

        this.ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                if (data.event) {
                    this.emit(data.event, data.data);
                } else {
                    // Raw metrics update
                    this.emit('training_metrics', data);
                }
            } catch (err) {
                console.error("WebSocket message parse error:", err);
            }
        };

        this.ws.onclose = () => {
            console.warn("⚠️ WebSocket Disconnected");
            this.emit('connection', { status: 'disconnected' });
            if (this.shouldReconnect) {
                this.scheduleReconnect();
            }
        };

        this.ws.onerror = (err) => {
            console.error("❌ WebSocket Error:", err);
            this.ws.close();
        };
    } catch(e) {
        console.error("Failed to setup WebSocket:", e);
        this.scheduleReconnect();
    }
  }

  scheduleReconnect() {
      const delay = Math.min(1000 * Math.pow(1.5, this.reconnectAttempts), 10000);
      this.reconnectAttempts++;
      console.log(`⏳ Reconnecting in ${delay}ms... (Attempt ${this.reconnectAttempts})`);
      setTimeout(() => this.connect(), delay);
  }

  on(event, callback) {
    if (!this.listeners[event]) this.listeners[event] = [];
    this.listeners[event].push(callback);
  }

  off(event, callback) {
    if (!this.listeners[event]) return;
    this.listeners[event] = this.listeners[event].filter(cb => cb !== callback);
  }

  emit(event, data) {
    if (this.listeners[event]) {
      this.listeners[event].forEach(cb => cb(data));
    }
  }
}

export const socket = new WebSocketService();
