"use client";
import { useEffect, useRef, useState } from "react";
import { Trash2, Square, Loader2 } from "lucide-react";

export default function LeafletMapClient({
  onBboxChange,
}: {
  onBboxChange: (bbox: any) => void;
}) {
  const mapRef = useRef<HTMLDivElement>(null);
  const mapInstance = useRef<any>(null);
  const rectangleRef = useRef<any>(null);
  const [mapLoaded, setMapLoaded] = useState(false);
  const [drawingMode, setDrawingMode] = useState(false);
  const [hasSelection, setHasSelection] = useState(false);
  const onBboxChangeRef = useRef(onBboxChange);
  const startPointRef = useRef<any>(null);
  const isInitializing = useRef(false);

  useEffect(() => {
    onBboxChangeRef.current = onBboxChange;
  }, [onBboxChange]);

  // Initialize map only once
  useEffect(() => {
    if (
      typeof window === "undefined" ||
      !mapRef.current ||
      mapInstance.current ||
      isInitializing.current
    )
      return;

    const initMap = () => {
      // @ts-ignore
      const L = window.L;
      if (!L || !mapRef.current) return;

      isInitializing.current = true;

      try {
        // Check if container already has a map instance
        const container = mapRef.current;

        // Remove any existing Leaflet instance from this container
        if ((container as any)._leaflet_id) {
          // Try to find and remove the old map
          try {
            const oldMapId = (container as any)._leaflet_id;
            // Clear the leaflet id
            delete (container as any)._leaflet_id;
          } catch (e) {
            console.warn("Could not clean up old map:", e);
          }
        }

        // Rondônia, Brazil bounds and center
        const rondoniaBounds = L.latLngBounds(
          [-13.5, -66.5], // Southwest corner
          [-8.0, -59.5]   // Northeast corner
        );
        
        const map = L.map(container, {
          zoomControl: false,
          attributionControl: false,
          maxBounds: rondoniaBounds,        // Restrict panning to Rondônia
          minZoom: 6,                       // Prevent zooming out too far
          maxZoom: 19,
        }).setView([-10.5, -63.0], 7);      // Center on Rondônia with zoom level 7

        mapInstance.current = map;

        L.tileLayer(
          "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
          {
            maxZoom: 19,
          },
        ).addTo(map);

        // Add custom zoom controls
        L.control.zoom({ position: "bottomright" }).addTo(map);

        setMapLoaded(true);
      } catch (error) {
        console.error("Error initializing map:", error);
        isInitializing.current = false;
      }
    };

    if (!document.getElementById("leaflet-css")) {
      const link = document.createElement("link");
      link.id = "leaflet-css";
      link.rel = "stylesheet";
      link.href = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css";
      document.head.appendChild(link);
    }

    if (!(window as any).L) {
      const script = document.createElement("script");
      script.src = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js";
      script.async = true;
      script.onload = initMap;
      document.head.appendChild(script);
    } else {
      initMap();
    }

    return () => {
      if (mapInstance.current) {
        try {
          mapInstance.current.remove();
        } catch (e) {
          console.warn("Error removing map:", e);
        }
        mapInstance.current = null;
      }
      isInitializing.current = false;
    };
  }, []); // Empty dependency array - initialize only once

  // Handle drawing mode separately
  useEffect(() => {
    const map = mapInstance.current;
    if (!map) return;

    const L = (window as any).L;
    if (!L) return;

    const handleMouseDown = (e: any) => {
      if (!drawingMode) return;

      // Prevent map panning while drawing
      map.dragging.disable();
      map.doubleClickZoom.disable();

      startPointRef.current = e.latlng;

      // Remove existing rectangle if any
      if (rectangleRef.current) {
        map.removeLayer(rectangleRef.current);
      }

      // Create new rectangle
      rectangleRef.current = L.rectangle([e.latlng, e.latlng], {
        color: "#3b82f6",
        weight: 2,
        fillOpacity: 0.2,
        fillColor: "#3b82f6",
      }).addTo(map);
    };

    const handleMouseMove = (e: any) => {
      if (!startPointRef.current || !rectangleRef.current || !drawingMode)
        return;
      rectangleRef.current.setBounds(
        L.latLngBounds(startPointRef.current, e.latlng),
      );
    };

    const handleMouseUp = (e: any) => {
      if (!startPointRef.current || !rectangleRef.current || !drawingMode)
        return;

      const bounds = rectangleRef.current.getBounds();

      // Only save if the rectangle has some area
      const north = bounds.getNorth();
      const south = bounds.getSouth();
      const east = bounds.getEast();
      const west = bounds.getWest();

      if (Math.abs(north - south) > 0.0001 && Math.abs(east - west) > 0.0001) {
        onBboxChangeRef.current({ north, south, east, west });
        setHasSelection(true);
      } else {
        // Remove tiny rectangles
        if (rectangleRef.current) {
          map.removeLayer(rectangleRef.current);
          rectangleRef.current = null;
        }
      }

      startPointRef.current = null;

      // Re-enable map controls
      map.dragging.enable();
      map.doubleClickZoom.enable();

      setDrawingMode(false);
    };

    if (drawingMode) {
      map.on("mousedown", handleMouseDown);
      map.on("mousemove", handleMouseMove);
      map.on("mouseup", handleMouseUp);
      map.getContainer().style.cursor = "crosshair";
    } else {
      map.off("mousedown", handleMouseDown);
      map.off("mousemove", handleMouseMove);
      map.off("mouseup", handleMouseUp);
      map.getContainer().style.cursor = "grab";

      // Re-enable map controls when not drawing
      map.dragging.enable();
      map.doubleClickZoom.enable();
    }

    return () => {
      map.off("mousedown", handleMouseDown);
      map.off("mousemove", handleMouseMove);
      map.off("mouseup", handleMouseUp);
    };
  }, [drawingMode]);

  // Fix map rendering on resize/layout changes (sidebar toggle fix)
  useEffect(() => {
    if (!mapInstance.current) return;

    const handleResize = () => {
      // Use setTimeout to ensure DOM has updated after sidebar animation
      setTimeout(() => {
        if (mapInstance.current) {
          mapInstance.current.invalidateSize();
        }
      }, 350); // Slightly longer than sidebar transition (300ms)
    };

    // Listen for window resize
    window.addEventListener("resize", handleResize);

    // Create a ResizeObserver to watch for container size changes
    let resizeObserver: ResizeObserver | null = null;
    if (mapRef.current) {
      resizeObserver = new ResizeObserver(() => {
        handleResize();
      });
      resizeObserver.observe(mapRef.current);
    }

    // Also invalidate size when component mounts
    handleResize();

    return () => {
      window.removeEventListener("resize", handleResize);
      if (resizeObserver) {
        resizeObserver.disconnect();
      }
    };
  }, [mapLoaded]);

  const handleClearSelection = () => {
    if (rectangleRef.current && mapInstance.current) {
      mapInstance.current.removeLayer(rectangleRef.current);
      rectangleRef.current = null;
      setHasSelection(false);
      onBboxChangeRef.current(null);
    }
  };

  return (
    <div className="relative w-full h-full min-h-[400px] md:min-h-[500px] lg:min-h-[600px] bg-muted overflow-hidden rounded-xl border border-border shadow-lg">
      <div ref={mapRef} className="w-full h-full z-0" />

      {/* Control Panel */}
      <div className="absolute top-3 right-3 md:top-4 md:right-4 z-[1000] flex flex-col gap-2">
        {/* Draw Rectangle Button */}
        <button
          onClick={() => setDrawingMode(!drawingMode)}
          disabled={!mapLoaded}
          className={`flex h-9 w-9 md:h-10 md:w-10 items-center justify-center rounded-lg border shadow-lg transition-all ${
            drawingMode
              ? "bg-primary text-primary-foreground border-primary scale-105"
              : "bg-card text-foreground border-border hover:bg-accent hover:scale-105"
          } disabled:opacity-50 disabled:cursor-not-allowed`}
          title={drawingMode ? "Cancel drawing" : "Draw selection area"}
        >
          <Square size={18} className={drawingMode ? "fill-current" : ""} />
        </button>

        {/* Clear Selection Button */}
        {hasSelection && (
          <button
            onClick={handleClearSelection}
            className="flex h-9 w-9 md:h-10 md:w-10 items-center justify-center rounded-lg border border-destructive/50 bg-card text-destructive shadow-lg hover:bg-destructive hover:text-destructive-foreground transition-all hover:scale-105 animate-in fade-in zoom-in duration-200"
            title="Clear selection"
          >
            <Trash2 size={18} />
          </button>
        )}
      </div>

      {/* Drawing Mode Indicator */}
      {drawingMode && (
        <div className="absolute top-3 left-3 md:top-4 md:left-4 z-[1000] bg-primary text-primary-foreground px-3 py-2 md:px-4 md:py-2 rounded-lg shadow-lg text-xs md:text-sm font-medium animate-in fade-in slide-in-from-left duration-300">
          Click and drag to select area
        </div>
      )}

      {/* Selection Info */}
      {hasSelection && !drawingMode && (
        <div className="absolute bottom-3 left-3 md:bottom-4 md:left-4 z-[1000] bg-card/95 backdrop-blur-sm border border-border text-foreground px-3 py-2 md:px-4 md:py-2 rounded-lg shadow-lg text-xs md:text-sm font-medium animate-in fade-in slide-in-from-left duration-300">
          ✓ Area selected
        </div>
      )}

      {/* Loading Overlay */}
      {!mapLoaded && (
        <div className="absolute inset-0 flex flex-col items-center justify-center bg-background/80 backdrop-blur-sm z-[2000] gap-3">
          <Loader2 className="w-8 h-8 md:w-10 md:h-10 text-primary animate-spin" />
          <p className="text-sm md:text-base text-muted-foreground font-medium">
            Loading map...
          </p>
        </div>
      )}
    </div>
  );
}
