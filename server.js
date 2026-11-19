const express = require('express')
const cors = require('cors')
const WebSocket = require('ws')

const app = express()
app.disable('x-powered-by')
app.use(cors())

const BBOX = { minLat: 3.5, maxLat: 6.5, minLon: 3.0, maxLon: 9.0 }
const MAX_AGE_MS = 6 * 60 * 60 * 1000 // forget vessels silent for 6 hours
const STALL_MS = 5 * 60 * 1000        // no messages for 5 minutes = stalled

const vessels = new Map()
let wsConnected = false
let ws = null
let lastMessageAt = 0

function connectAIS() {
  if (wsConnected || ws) return
  const apiKey = process.env.AIS_STREAM_API_KEY
  if (!apiKey) { console.error('[AIS] No API key set'); return }

  ws = new WebSocket('wss://stream.aisstream.io/v0/stream')

  ws.on('open', () => {
    wsConnected = true
    lastMessageAt = Date.now()
    console.log('[AIS] Connected to aisstream.io')
    ws.send(JSON.stringify({
      APIKey: apiKey,
      BoundingBoxes: [[[BBOX.minLat, BBOX.minLon], [BBOX.maxLat, BBOX.maxLon]]],
      FilterMessageTypes: ['PositionReport', 'ShipStaticData'],
    }))
  })

  ws.on('message', (data) => {
    lastMessageAt = Date.now()
    try {
      const msg = JSON.parse(data.toString())
      const mmsi = String(msg.MetaData?.MMSI ?? '')
      if (!mmsi) return

      if (msg.MessageType === 'PositionReport') {
        const pos = msg.Message?.PositionReport
        if (!pos) return
        const existing = vessels.get(mmsi) ?? {
          mmsi, name: msg.MetaData?.ShipName?.trim() || mmsi,
          lat: 0, lon: 0, speed: 0, course: 0, shipType: 0, timestamp: '',
        }
        vessels.set(mmsi, {
          ...existing,
          lat: pos.Latitude,
          lon: pos.Longitude,
          speed: pos.SpeedOverGround ?? 0,
          course: pos.CourseOverGround ?? 0,
          timestamp: msg.MetaData?.time_utc ?? new Date().toISOString(),
          seenAt: Date.now(),
        })
      }

      if (msg.MessageType === 'ShipStaticData') {
        const s = msg.Message?.ShipStaticData
        const existing = vessels.get(mmsi)
        if (s && existing) {
          existing.name = s.Name?.trim() || existing.name
          existing.shipType = s.Type ?? existing.shipType
        }
      }
    } catch (e) {}
  })

  ws.on('close', () => {
    wsConnected = false
    ws = null
    console.log('[AIS] Disconnected, reconnecting in 10s')
    setTimeout(connectAIS, 10000)
  })

  ws.on('error', (err) => {
    console.error('[AIS] Error:', err.message)
    ws?.close()
  })
}

// Forget vessels not heard from in MAX_AGE_MS
setInterval(() => {
  const cutoff = Date.now() - MAX_AGE_MS
  for (const [mmsi, v] of vessels) {
    if (!v.seenAt || v.seenAt < cutoff) vessels.delete(mmsi)
  }
}, 10 * 60 * 1000)

// Reconnect if the stream goes quiet without closing
setInterval(() => {
  if (ws && wsConnected && Date.now() - lastMessageAt > STALL_MS) {
    console.warn('[AIS] Stream stalled, reconnecting')
    ws.terminate()
  }
}, 60 * 1000)

const isLive = () => wsConnected && Date.now() - lastMessageAt < STALL_MS

app.get('/vessels', (req, res) => {
  res.json({ vessels: Array.from(vessels.values()), count: vessels.size, connected: isLive(), bbox: BBOX })
})

app.get('/health', (req, res) => {
  res.json({ ok: true, connected: isLive(), count: vessels.size })
})

const PORT = process.env.PORT || 3001
app.listen(PORT, () => {
  console.log(`[AIS Bridge] Listening on port ${PORT}`)
  connectAIS()
})
