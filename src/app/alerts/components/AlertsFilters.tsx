"use client";

import type { AlertSeverity } from "@/types/alert";



export default function AlertsFilters({
  value,
  onChange,
}: {
  value: "ALL" | AlertSeverity;
  onChange: (v: "ALL" | AlertSeverity) => void;
}) {
  return (
    <div className="bg-card border border-border rounded-2xl p-2 shadow-sm flex flex-wrap gap-2">
      {["ALL", "LOW", "WARNING", "CRITICAL"].map((s) => (
        <button
          key={s}
          onClick={() => onChange(s as any)}
          className={`
            px-5 py-2 rounded-xl text-xs font-black uppercase tracking-widest transition-all
            ${
              value === s
                ? "bg-primary text-primary-foreground shadow-lg shadow-primary/20"
                : "bg-background text-muted-foreground hover:bg-muted border border-border"
            }
          `}
        >
          {s}
        </button>
      ))}
    </div>
  );
}
