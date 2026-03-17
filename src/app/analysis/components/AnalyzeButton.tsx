"use client";
import { Scan, Loader2, MapPin } from "lucide-react";


export default function AnalyzeButton({
  loading,
  disabled,
  onClick,
}: {
  loading: boolean;
  disabled?: boolean;
  onClick: () => void;
}) {
  return (
    <div className="w-full">
      <button
        onClick={onClick}
        disabled={loading || disabled}
        className={`w-full group relative flex items-center justify-center gap-3 px-6 py-4 rounded-xl border transition-all duration-200 text-xs font-bold tracking-widest uppercase ${
          loading || disabled
            ? "bg-muted border-border text-muted-foreground cursor-not-allowed"
            : "bg-primary text-primary-foreground border-primary hover:opacity-90 active:scale-[0.98]"
        }`}
      >
        {loading ? (
          <>
            <Loader2 className="w-4 h-4 animate-spin" />
            <span>Scanning Region...</span>
          </>
        ) : disabled ? (
          <>
            <MapPin className="w-4 h-4 opacity-40" />
            <span>Select Area</span>
          </>
        ) : (
          <>
            <Scan className="w-4 h-4" />
            <span>Run Analysis</span>
          </>
        )}
      </button>
      {!loading && disabled && (
        <p className="text-[10px] text-center mt-2 text-muted-foreground tracking-tight">
          Awaiting Area of Interest (AOI) selection
        </p>
      )}
    </div>
  );
}
