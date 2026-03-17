"use client";


import { useState } from "react";
import {
  ReactCompareSlider,
  ReactCompareSliderImage,
} from "react-compare-slider";
import { Layers, Columns, Eye } from "lucide-react";
import type { AnalyzeResponse } from "@/types/analysis";

export default function VisualResultsCard({
  result,
}: {
  result: AnalyzeResponse | null;
}) {
  const [displayMode, setDisplayMode] = useState<"COMPARISON" | "OVERLAY">(
    "COMPARISON",
  );
  const [overlayType, setOverlayType] = useState<
    "CHANGE_MASK" | "NDVI" | "NONE"
  >("CHANGE_MASK");
  const [opacity, setOpacity] = useState(55);

  if (!result) return null;

  const overlayUrl =
    overlayType === "CHANGE_MASK"
      ? result.images?.changeMaskUrl
      : overlayType === "NDVI"
        ? result.images?.ndviHeatmapUrl
        : null;

  return (
    <div className="rounded-2xl border border-border bg-card overflow-hidden shadow-2xl animate-in fade-in slide-in-from-bottom-4 duration-500">
      <div className="flex flex-col md:flex-row items-center justify-between p-4 gap-4 border-b border-border bg-muted/30">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-primary/10 rounded-lg">
            <Eye className="w-4 h-4 text-primary" />
          </div>
          <div>
            <h3 className="text-xs font-bold tracking-[0.2em] uppercase text-foreground">
              Visual Analysis
            </h3>
            <p className="text-[10px] text-muted-foreground uppercase font-medium">
              Post-Processing Output
            </p>
          </div>
        </div>

        <div className="flex bg-muted p-1 rounded-xl border border-border">
          <button
            onClick={() => setDisplayMode("COMPARISON")}
            className={`flex items-center gap-2 px-4 py-1.5 rounded-lg text-[10px] font-black tracking-widest transition-all ${
              displayMode === "COMPARISON"
                ? "bg-background text-foreground shadow-sm"
                : "text-muted-foreground hover:text-foreground"
            }`}
          >
            <Columns size={12} /> COMPARISON
          </button>
          <button
            onClick={() => setDisplayMode("OVERLAY")}
            className={`flex items-center gap-2 px-4 py-1.5 rounded-lg text-[10px] font-black tracking-widest transition-all ${
              displayMode === "OVERLAY"
                ? "bg-background text-foreground shadow-sm"
                : "text-muted-foreground hover:text-foreground"
            }`}
          >
            <Layers size={12} /> OVERLAYS
          </button>
        </div>
      </div>

      <div className="p-5">
        {displayMode === "OVERLAY" && (
          <div className="mb-4 flex flex-wrap items-center justify-between gap-4 animate-in fade-in duration-300">
            <div className="flex gap-2">
              {(["CHANGE_MASK", "NDVI", "NONE"] as const).map((type) => (
                <button
                  key={type}
                  onClick={() => setOverlayType(type)}
                  className={`px-3 py-1.5 rounded-md text-[10px] font-bold border transition-all ${
                    overlayType === type
                      ? "bg-primary text-primary-foreground border-primary"
                      : "bg-muted border-border text-muted-foreground hover:bg-accent"
                  }`}
                >
                  {type.replace("_", " ")}
                </button>
              ))}
            </div>
            <div className="flex items-center gap-3 bg-muted px-3 py-1.5 rounded-lg border border-border">
              <span className="text-[10px] text-muted-foreground font-bold uppercase">
                Opacity
              </span>
              <input
                type="range"
                min="0"
                max="100"
                value={opacity}
                onChange={(e) => setOpacity(parseInt(e.target.value))}
                className="w-24 accent-primary h-1 rounded-full cursor-pointer"
              />
              <span className="text-[10px] font-mono text-foreground w-8">
                {opacity}%
              </span>
            </div>
          </div>
        )}

        <div className="relative rounded-xl border border-border bg-black aspect-video overflow-hidden shadow-inner">
          {displayMode === "COMPARISON" ? (
            <ReactCompareSlider
              itemOne={
                <ReactCompareSliderImage
                  src={result.images?.beforeImageUrl}
                  alt="Before"
                  className="object-cover"
                />
              }
              itemTwo={
                <ReactCompareSliderImage
                  src={result.images?.afterImageUrl}
                  alt="After"
                  className="object-cover"
                />
              }
            />
          ) : (
            <div className="relative h-full w-full">
              <img
                src={result.images?.afterImageUrl}
                alt="Base"
                className="absolute inset-0 w-full h-full object-cover"
              />
              {overlayUrl && overlayType !== "NONE" && (
                <img
                  src={overlayUrl}
                  className="absolute inset-0 w-full h-full object-cover transition-opacity duration-300"
                  style={{
                    opacity: opacity / 100,
                    mixBlendMode:
                      overlayType === "CHANGE_MASK" ? "screen" : "normal",
                  }}
                />
              )}
            </div>
          )}
          <div className="absolute bottom-4 left-4">
            <div className="px-3 py-1.5 rounded-md bg-background/80 backdrop-blur-md border border-border flex items-center gap-2">
              <div
                className={`w-1.5 h-1.5 rounded-full ${displayMode === "COMPARISON" ? "bg-blue-500" : "bg-emerald-500"} animate-pulse`}
              />
              <span className="text-[9px] font-black tracking-widest text-foreground uppercase">
                {displayMode === "COMPARISON"
                  ? "Dual-Temporal Sync"
                  : `${overlayType} ACTIVE`}
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
