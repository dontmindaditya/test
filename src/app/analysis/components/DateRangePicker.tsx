"use client";

export default function DateRangePicker({
  startDate,
  endDate,
  onChange,
}: {
  startDate: string;
  endDate: string;
  onChange: (start: string, end: string) => void;
}) {
  const presets = [
    { label: "7D", days: 7 },
    { label: "30D", days: 30 },
    { label: "1Y", days: 365 },
  ];

  return (
    <div className="rounded-2xl border border-border bg-card p-5 shadow-sm">
      <p className="text-xs uppercase tracking-widest text-muted-foreground mb-3">
        Date Range
      </p>
      <div className="flex gap-2 mb-4">
        {presets.map((preset) => (
          <button
            key={preset.label}
            onClick={() => {
              const end = new Date();
              const start = new Date();
              start.setDate(end.getDate() - preset.days);
              onChange(
                start.toISOString().slice(0, 10),
                end.toISOString().slice(0, 10),
              );
            }}
            className="px-3 py-1 text-[10px] font-bold rounded-md bg-muted border border-border text-foreground hover:bg-accent transition-all"
          >
            {preset.label}
          </button>
        ))}
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="space-y-1">
          <label className="text-[10px] text-muted-foreground font-bold uppercase">
            Start
          </label>
          <input
            type="date"
            value={startDate}
            onChange={(e) => onChange(e.target.value, endDate)}
            className="w-full rounded-lg border border-border bg-background px-3 py-2 text-xs text-foreground outline-none focus:ring-1 focus:ring-primary"
          />
        </div>
        <div className="space-y-1">
          <label className="text-[10px] text-muted-foreground font-bold uppercase">
            End
          </label>
          <input
            type="date"
            value={endDate}
            onChange={(e) => onChange(startDate, e.target.value)}
            className="w-full rounded-lg border border-border bg-background px-3 py-2 text-xs text-foreground outline-none focus:ring-1 focus:ring-primary"
          />
        </div>
      </div>
    </div>
  );
}
