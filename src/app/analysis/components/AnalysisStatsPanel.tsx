"use client";
import type { BoundingBox } from "@/types/geo";
import type { AnalyzeResponse } from "@/types/analysis";

export default function AnalysisStatsPanel({
  bbox,
  result,
}: {
  bbox: BoundingBox | null;
  result: AnalyzeResponse | null;
  error: string | null;
}) {
  return (
    <div className="relative overflow-hidden rounded-2xl border border-border bg-card p-6 shadow-sm">
      <div className="flex items-center justify-between mb-6">
        <p className="text-[10px] font-black uppercase tracking-[0.2em] text-muted-foreground">
          Analysis Stats
        </p>
        <div
          className={`h-2 w-2 rounded-full animate-pulse ${result?.status === "COMPLETED" ? "bg-emerald-500" : "bg-blue-500"}`}
        />
      </div>

      <div className="space-y-3">
        {[
          {
            label: "AOI Status",
            val: bbox ? "✓ Selected" : "✗ None",
            color: bbox
              ? "text-emerald-600 dark:text-emerald-400"
              : "text-rose-600 dark:text-rose-400",
          },
          {
            label: "Status",
            val: result?.status ?? "IDLE",
            color: "text-foreground",
          },
          {
            label: "Severity",
            val: result?.severity ?? "--",
            color: result
              ? "text-orange-600 dark:text-orange-400"
              : "text-muted-foreground/40",
          },
        ].map((item, i) => (
          <div
            key={i}
            className="flex justify-between items-center p-3 rounded-xl bg-muted/50 border border-border"
          >
            <span className="text-muted-foreground text-[11px] font-bold uppercase">
              {item.label}
            </span>
            <span className={`text-xs font-black tracking-tight ${item.color}`}>
              {item.val}
            </span>
          </div>
        ))}

        {result && (
          <div className="p-4 rounded-xl bg-muted/50 border border-border space-y-3">
            <div className="flex justify-between">
              <span className="text-muted-foreground text-[11px] font-bold uppercase">
                Mean NDVI
              </span>
              <span className="text-emerald-600 dark:text-emerald-400 font-black font-mono">
                {result.ndvi.mean.toFixed(3)}
              </span>
            </div>
            <div className="flex justify-between text-[10px] font-mono text-muted-foreground border-t border-border pt-2">
              <span>MIN: {result.ndvi.min.toFixed(2)}</span>
              <span>MAX: {result.ndvi.max.toFixed(2)}</span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
