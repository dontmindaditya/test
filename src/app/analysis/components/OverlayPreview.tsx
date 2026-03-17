"use client";
import { useState } from "react";
import type { AnalyzeResponse } from "@/types/analysis";


export default function OverlayPreview({
  result,
}: {
  result: AnalyzeResponse | null;
}) {
  const [mode, setMode] = useState<"NONE" | "CHANGE_MASK" | "NDVI">(
    "CHANGE_MASK",
  );
  const [opacity, setOpacity] = useState(60);

  const baseImage = result?.images?.afterImageUrl;
  const overlayUrl =
    mode === "CHANGE_MASK"
      ? result?.images?.changeMaskUrl
      : mode === "NDVI"
        ? result?.images?.ndviHeatmapUrl
        : null;

  return (
    <div className="rounded-2xl border border-border bg-card p-5 shadow-sm">
      <div className="flex flex-col sm:flex-row justify-between gap-4 mb-4">
        <div>
          <p className="text-[10px] font-black uppercase tracking-widest text-muted-foreground">
            Overlay Control
          </p>
          <p className="text-xs text-muted-foreground">
            Toggle analysis layers
          </p>
        </div>
        <div className="flex bg-muted p-1 rounded-lg border border-border">
          {["CHANGE_MASK", "NDVI", "NONE"].map((m) => (
            <button
              key={m}
              onClick={() => setMode(m as any)}
              className={`px-3 py-1 text-[10px] font-bold rounded-md transition-all ${
                mode === m
                  ? "bg-background text-foreground shadow-sm"
                  : "text-muted-foreground hover:text-foreground"
              }`}
            >
              {m.replace("_", " ")}
            </button>
          ))}
        </div>
      </div>

      <div className="relative aspect-video rounded-xl border border-border bg-muted overflow-hidden">
        {!baseImage ? (
          <div className="absolute inset-0 flex items-center justify-center text-muted-foreground text-xs uppercase font-bold">
            Awaiting Analysis
          </div>
        ) : (
          <>
            <img
              src={baseImage}
              className="absolute inset-0 w-full h-full object-cover"
              alt="Satellite"
            />
            {overlayUrl && mode !== "NONE" && (
              <img
                src={overlayUrl}
                style={{
                  opacity: opacity / 100,
                  mixBlendMode: mode === "CHANGE_MASK" ? "screen" : "normal",
                }}
                className="absolute inset-0 w-full h-full object-cover pointer-events-none"
                alt="Overlay"
              />
            )}
            <div className="absolute bottom-3 left-3 bg-background/80 backdrop-blur-md border border-border p-2 rounded-lg text-[10px] font-bold">
              Layer: {mode} • {opacity}%
            </div>
          </>
        )}
      </div>

      <div className="mt-4 flex items-center gap-4">
        <span className="text-[10px] font-bold text-muted-foreground uppercase">
          Opacity
        </span>
        <input
          type="range"
          value={opacity}
          onChange={(e) => setOpacity(Number(e.target.value))}
          className="flex-1 accent-primary"
        />
      </div>
    </div>
  );
}
