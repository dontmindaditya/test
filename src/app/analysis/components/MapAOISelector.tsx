"use client";

import dynamic from "next/dynamic";
import type { BoundingBox } from "@/types/geo";
import type { AnalyzeResponse } from "@/types/analysis";

const LeafletMapClient = dynamic(() => import("./LeafletMapClient"), {
  ssr: false,
});


export default function MapAOISelector({
  onBboxChange,
}: {
  onBboxChange: (bbox: BoundingBox) => void;
  result: AnalyzeResponse | null;
}) {
  return (
    <div className="bg-[#071225]">
      <div className="px-5 py-3 border-b border-white/5 flex justify-between items-center">
        <h3 className="text-[10px] uppercase tracking-[0.2em] font-bold text-white/60">
          Satellite Area Selection
        </h3>
        <span className="text-[10px] text-blue-400 font-bold animate-pulse">
          LIVE FEED ACCESS
        </span>
      </div>
      <div className="h-[450px]">
        <LeafletMapClient onBboxChange={onBboxChange} />
      </div>
    </div>
  );
}
