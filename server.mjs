import express from "express";
import fetch from "node-fetch";

const app = express();
const PORT = 3000;

app.use(express.static("public"));

const USER_AGENT = "(temp-extremes-app/1.0, iains@duck.com)";

// How many stations to load into the master catalog.
const MAX_STATIONS = 10000;

// How many stations to scan per refresh cycle.
const STATIONS_PER_CYCLE = 1000;

// How many observation requests to run at once inside each cycle.
const BATCH_SIZE = 10;

// How often to refresh observations.
const REFRESH_INTERVAL_MS = 5 * 60 * 1000;

// How often to refresh the station catalog itself.
const STATION_REFRESH_INTERVAL_MS = 24 * 60 * 60 * 1000;

// Ignore observations older than this.
const MAX_OBSERVATION_AGE_MS = 2 * 60 * 60 * 1000;

let cachedStations = [];
let stationsLastUpdated = 0;
let stationOffset = 0;

// This stores the best result from the most recent completed sweep
// across all station slices.
let sweepState = {
  hottest: null,
  coldest: null,
  scannedStations: 0,
  validObservations: 0,
  slicesCompleted: 0,
  sweepStartedAt: null
};

let latestData = {
  scannedStations: 0,
  validObservations: 0,
  updatedAt: null,
  hottest: null,
  coldest: null,
  loading: true,
  error: null,
  cycleStationsScanned: 0,
  totalStationsAvailable: 0,
  slicesCompleted: 0,
  sweepStartedAt: null
};

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchJson(url, attempts = 3) {
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      const res = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          "Accept": "application/geo+json"
        }
      });

      if (!res.ok) {
        throw new Error(`HTTP ${res.status} for ${url}`);
      }

      return await res.json();
    } catch (error) {
      if (attempt === attempts) {
        throw error;
      }

      console.log(`Retry ${attempt} failed for ${url}`);
      await sleep(1000 * attempt);
    }
  }
}

async function getAllStations(limit = MAX_STATIONS) {
  let url = "https://api.weather.gov/stations?limit=100";
  const stations = [];

  while (url && stations.length < limit) {
    const data = await fetchJson(url);

    if (Array.isArray(data.features)) {
      for (const feature of data.features) {
        const props = feature.properties || {};
        const coords = feature.geometry?.coordinates || [];

        stations.push({
          id: props.stationIdentifier,
          name: props.name || props.stationIdentifier,
          longitude: coords[0] ?? null,
          latitude: coords[1] ?? null
        });

        if (stations.length >= limit) break;
      }
    }

    url = data.pagination?.next || null;
  }

  return stations;
}

async function ensureStationsLoaded() {
  const needsRefresh =
    cachedStations.length === 0 ||
    Date.now() - stationsLastUpdated > STATION_REFRESH_INTERVAL_MS;

  if (!needsRefresh) {
    return cachedStations;
  }

  console.log("Refreshing station catalog...");
  cachedStations = await getAllStations(MAX_STATIONS);
  stationsLastUpdated = Date.now();

  // Reset offset if needed
  if (stationOffset >= cachedStations.length) {
    stationOffset = 0;
  }

  console.log(`Loaded ${cachedStations.length} stations.`);
  return cachedStations;
}

function getStationSlice(stations) {
  if (stations.length === 0) return [];

  if (stations.length <= STATIONS_PER_CYCLE) {
    return {
      slice: stations,
      wrapped: true
    };
  }

  const start = stationOffset;
  const end = Math.min(start + STATIONS_PER_CYCLE, stations.length);
  const slice = stations.slice(start, end);

  stationOffset = end;

  let wrapped = false;
  if (stationOffset >= stations.length) {
    stationOffset = 0;
    wrapped = true;
  }

  return { slice, wrapped };
}

function isFresh(timestampString) {
  if (!timestampString) return false;
  const obsTime = new Date(timestampString).getTime();
  if (Number.isNaN(obsTime)) return false;
  return Date.now() - obsTime <= MAX_OBSERVATION_AGE_MS;
}

async function getLatestObservation(station) {
  try {
    const url = `https://api.weather.gov/stations/${station.id}/observations/latest`;
    const res = await fetch(url, {
      headers: {
        "User-Agent": USER_AGENT,
        "Accept": "application/geo+json"
      }
    });

    if (!res.ok) return null;

    const data = await res.json();
    const props = data.properties || {};

    if (props.temperature?.value == null) return null;
    if (!isFresh(props.timestamp)) return null;

    return {
      stationId: station.id,
      stationName: station.name,
      latitude: station.latitude,
      longitude: station.longitude,
      temperatureC: props.temperature.value,
      temperatureF: (props.temperature.value * 9) / 5 + 32,
      timestamp: props.timestamp,
      textDescription: props.textDescription || "",
      forecast: props.forecast || null
    };
  } catch {
    return null;
  }
}

async function getAllLatestObservations(stations) {
  const results = [];

  for (let i = 0; i < stations.length; i += BATCH_SIZE) {
    const batch = stations.slice(i, i + BATCH_SIZE);
    const batchResults = await Promise.all(batch.map(getLatestObservation));
    results.push(...batchResults.filter(Boolean));

    if (i + BATCH_SIZE < stations.length) {
      await sleep(300);
    }
  }

  return results;
}

function updateSweepExtremes(observations) {
  for (const obs of observations) {
    if (!sweepState.hottest || obs.temperatureC > sweepState.hottest.temperatureC) {
      sweepState.hottest = obs;
    }

    if (!sweepState.coldest || obs.temperatureC < sweepState.coldest.temperatureC) {
      sweepState.coldest = obs;
    }
  }
}

async function getLocationName(lat, lon) {
  try {
    if (lat == null || lon == null) return null;

    const url = `https://nominatim.openstreetmap.org/reverse?lat=${lat}&lon=${lon}&format=jsonv2&zoom=10&addressdetails=1`;

    const res = await fetch(url, {
      headers: {
        "User-Agent": USER_AGENT
      }
    });

    if (!res.ok) return null;

    const data = await res.json();
    const address = data.address || {};

    const place =
      address.city ||
      address.town ||
      address.village ||
      address.municipality ||
      address.county ||
      address.state_district ||
      "Unknown area";

    const state =
      address.state ||
      address.region ||
      "";

    return state ? `${place}, ${state}` : place;
  } catch {
    return null;
  }
}

function resetSweep() {
  sweepState = {
    hottest: null,
    coldest: null,
    scannedStations: 0,
    validObservations: 0,
    slicesCompleted: 0,
    sweepStartedAt: new Date().toISOString()
  };
}

async function finalizeSweep(totalStationsAvailable) {
  if (!sweepState.hottest || !sweepState.coldest) {
    throw new Error("No valid observations found in completed sweep.");
  }

  const hottestLocation = await getLocationName(
    sweepState.hottest.latitude,
    sweepState.hottest.longitude
  );

  const coldestLocation = await getLocationName(
    sweepState.coldest.latitude,
    sweepState.coldest.longitude
  );

  sweepState.hottest.locationName =
    hottestLocation ||
    sweepState.hottest.stationName ||
    sweepState.hottest.stationId;

  sweepState.coldest.locationName =
    coldestLocation ||
    sweepState.coldest.stationName ||
    sweepState.coldest.stationId;

  latestData = {
    scannedStations: sweepState.scannedStations,
    validObservations: sweepState.validObservations,
    updatedAt: new Date().toISOString(),
    hottest: sweepState.hottest,
    coldest: sweepState.coldest,
    loading: false,
    error: null,
    cycleStationsScanned: STATIONS_PER_CYCLE,
    totalStationsAvailable,
    slicesCompleted: sweepState.slicesCompleted,
    sweepStartedAt: sweepState.sweepStartedAt
  };
}

async function refreshData() {
  try {
    console.log("Refreshing weather data...");

    const allStations = await ensureStationsLoaded();

    if (!allStations.length) {
      throw new Error("No stations loaded.");
    }

    // If starting a brand new sweep, initialize it
    if (sweepState.sweepStartedAt === null) {
      resetSweep();
    }

    const { slice, wrapped } = getStationSlice(allStations);

    console.log(
      `Scanning ${slice.length} stations this cycle. Offset now: ${stationOffset}`
    );

    const observations = await getAllLatestObservations(slice);

    sweepState.scannedStations += slice.length;
    sweepState.validObservations += observations.length;
    sweepState.slicesCompleted += 1;

    updateSweepExtremes(observations);

    // If we've wrapped, that means we've completed a full sweep of the catalog
    if (wrapped) {
      console.log("Completed full station sweep. Finalizing extremes...");
      await finalizeSweep(allStations.length);
      console.log("Full sweep complete.");

      // Start the next sweep fresh
      resetSweep();
    } else {
      // During an in-progress sweep, keep serving the last completed sweep result
      latestData = {
        ...latestData,
        loading: latestData.hottest === null || latestData.coldest === null,
        error: null,
        cycleStationsScanned: slice.length,
        totalStationsAvailable: allStations.length
      };
    }
  } catch (error) {
    console.error("Refresh failed:", error.message);
    latestData = {
      ...latestData,
      loading: false,
      error: error.message
    };
  }
}

app.get("/extremes", (req, res) => {
  res.json(latestData);
});

app.listen(PORT, async () => {
  console.log(`http://localhost:${PORT}`);
  resetSweep();
  await refreshData();
  setInterval(refreshData, REFRESH_INTERVAL_MS);
});