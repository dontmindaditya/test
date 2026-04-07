"use client";

import "leaflet/dist/leaflet.css";
import {
  MapContainer,
  TileLayer,
  Marker,
  LayersControl,
  useMap,
  useMapEvents,
} from "react-leaflet";
import type { LatLngLiteral } from "leaflet";
import { useEffect, useMemo, useState } from "react";

const DEFAULT_CENTER: LatLngLiteral = { lat: -10.5, lng: -63.0 };

const satelliteUrl =
  "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}";
const streetsUrl = "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png";
const terrainUrl = "https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png";
const RONDONIA_BOUNDS: [[number, number], [number, number]] = [
  [-13.5, -66.5],
  [-8.0, -59.5],
];

function ClickHandler({ onPick }: { onPick: (value: LatLngLiteral) => void }) {
  useMapEvents({
    click(e) {
      onPick({ lat: e.latlng.lat, lng: e.latlng.lng });
    },
  });
  return null;
}

function MapPanTo({ center }: { center?: { lat: number; lon: number } | null }) {
  const map = useMap();
  const lat = center?.lat;
  const lon = center?.lon;

  useEffect(() => {
    if (!center) return;
    map.setView({ lat: center.lat, lng: center.lon }, map.getZoom(), {
      animate: true,
    });
  }, [lat, lon, map, center]);

  return null;
}

export default function LeafletMapSelectorClient({
  value,
  onChange,
  center,
  height = 460,
}: {
  value: { lat: number; lon: number } | null;
  onChange: (value: { lat: number; lon: number }) => void;
  center?: { lat: number; lon: number } | null;
  height?: number;
}) {
  const [icon, setIcon] = useState<any>(null);

  useEffect(() => {
    let mounted = true;
    async function loadIcon() {
      const leaflet = await import("leaflet");
      if (!mounted) return;
      const next = leaflet.icon({
        iconUrl:
          "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png",
        iconRetinaUrl:
          "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png",
        shadowUrl:
          "https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png",
        iconSize: [25, 41],
        iconAnchor: [12, 41],
      });
      setIcon(next);
    }
    loadIcon();
    return () => {
      mounted = false;
    };
  }, []);

  const markerPosition = value
    ? ({ lat: value.lat, lng: value.lon } as LatLngLiteral)
    : null;

  return (
    <div className="bg-card border border-border rounded-2xl overflow-hidden relative z-0">
      <div className="px-5 py-3 border-b border-border/50 flex items-center justify-between">
        <div>
          <p className="text-[10px] uppercase tracking-[0.2em] font-bold text-muted-foreground">
            Map Selector
          </p>
          <p className="text-xs text-muted-foreground">
            Click anywhere in Rondônia to set coordinates
          </p>
        </div>
        <span className="text-[10px] text-primary font-bold">LIVE</span>
      </div>

      <div style={{ height }} className="relative z-0">
        <MapContainer
          center={DEFAULT_CENTER}
          zoom={7}
          minZoom={6}
          maxZoom={18}
          maxBounds={RONDONIA_BOUNDS}
          maxBoundsViscosity={0.8}
          scrollWheelZoom
          className="h-full w-full z-0"
        >
          <LayersControl position="topright">
            <LayersControl.BaseLayer checked name="Satellite">
              <TileLayer url={satelliteUrl} />
            </LayersControl.BaseLayer>
            <LayersControl.BaseLayer name="Streets">
              <TileLayer url={streetsUrl} />
            </LayersControl.BaseLayer>
            <LayersControl.BaseLayer name="Terrain">
              <TileLayer url={terrainUrl} />
            </LayersControl.BaseLayer>
          </LayersControl>

          <ClickHandler
            onPick={(picked) =>
              onChange({ lat: picked.lat, lon: picked.lng })
            }
          />
          <MapPanTo center={center} />

          {markerPosition && icon && (
            <Marker position={markerPosition} icon={icon} />
          )}
        </MapContainer>
      </div>
    </div>
  );
}
