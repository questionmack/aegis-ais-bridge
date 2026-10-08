const express = require('express')
const cors = require('cors')
const WebSocket = require('ws')

const app = express()
app.use(cors())

const BBOX = {
  minLat: 3.5,
  maxLat: 6.5,
  minLon: 3.0,
  maxLon: 9.0,
}

const vessels = new Map()
let wsConnected = false
let ws = null

function connectAIS() {
  if (wsConnected || ws) return

  const apiKey = process.env.AIS_STREAM_API_KEY
  if (!apiKey) {
    console.error('[AIS] No API key set')
    return
  }

  ws = new WebSocket('wss://stream.aisstream.io/v0/stream')

  ws.on('open', () => {
    wsConnected = true
    console.log('[AIS] Connected to aisstream.io')
    ws.send(JSON.stringify({
      APIKey: apiKey,
      BoundingBoxes: [[
        [BBOX.minLat, BBOX.minLon],
        [BBOX.maxLat, BBOX.maxLon],
      ]],
      FilterMessageTypes: ['PositionReport', 'ShipStaticData'],
    }))
  })

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString())
      const mmsi = String(msg.MetaData?.MMSI ?? '')
      if (!mmsi) return

      if (msg.MessageType === 'PositionReport') {
        const pos = msg.Message?.PositionReport
        if (!pos) return
        const existing = vessels.get(mmsi) ?? {
          mmsi, name: msg.MetaData?.ShipName?.trim() || mmsi,
          lat: 0, lon: 0, speed: 0, course: 0, shipType: 0, timestamp: ''
        }
        vessels.set(mmsi, {
          ...existing,
          lat: pos.Latitude,
          lon: pos.Longitude,
          speed: pos.SpeedOverGround ?? 0,
          course: pos.CourseOverGround ?? 0,
          timestamp: msg.MetaData?.time_utc ?? new Date().toISOString(),
        })
      }

      if (msg.MessageType === 'ShipStaticData') {
        const s = msg.Message?.ShipStaticData
        if (!s) return
        const existing = vessels.get(mmsi)
        if (existing) {
          existing.name = s.Name?.trim() || existing.name
          existing.shipType = s.Type ?? existing.shipType
        }
      }
    } catch (e) {}
  })

  ws.on('close', () => {
    wsConnected = false
    ws = null
    console.log('[AIS] Disconnected — reconnecting in 10s')
    setTimeout(connectAIS, 10000)
  })

  ws.on('error', (err) => {
    console.error('[AIS] Error:', err.message)
    ws?.close()
  })
}

app.get('/vessels', (req, res) => {
  res.json({
    vessels: Array.from(vessels.values()),
    count: vessels.size,
    connected: wsConnected,
    bbox: BBOX,
  })
})

app.get('/health', (req, res) => {
  res.json({ ok: true, connected: wsConnected, count: vessels.size })
})

const PORT = process.env.PORT || 3001
app.listen(PORT, () => {
  console.log(`[AIS Bridge] Listening on port ${PORT}`)
  connectAIS()
})
