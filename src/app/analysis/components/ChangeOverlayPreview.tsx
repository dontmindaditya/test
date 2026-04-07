import type { AnalyzeResponse } from "@/types/analysis";

export default function ChangeOverlayPreview({
  result,
}: {
  result: AnalyzeResponse | null;
}) {
  return (
    <div className="rounded-2xl border border-white/10 bg-[#071225] p-5 shadow-xl">
      <p className="text-xs uppercase tracking-widest text-white/40">
        Overlay Preview
      </p>

      <div className="mt-4 h-[240px] rounded-xl border border-white/10 bg-black/30 flex items-center justify-center text-white/60">
        {result?.images?.changeMaskUrl
          ? "Overlay Image Loaded"
          : "Overlay will appear here after analysis"}
      </div>
    </div>
  );
}
