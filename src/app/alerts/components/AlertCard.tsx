import type { Alert } from "@/types/alert";

export default function AlertCard({ alert }: { alert: Alert }) {
  return (
    <div className="bg-card rounded-2xl p-5 shadow-sm border border-border flex flex-col h-full hover:border-primary/50 transition-colors">
      <div className="flex items-start justify-between gap-4">
        <h3 className="font-bold text-foreground leading-tight">
          {alert.title}
        </h3>
        <span
          className={`
            shrink-0 px-3 py-1 rounded-full text-[10px] font-black uppercase tracking-tighter border
            ${
              alert.severity === "CRITICAL"
                ? "bg-red-500/10 text-red-600 dark:text-red-400 border-red-500/20"
                : alert.severity === "WARNING"
                  ? "bg-yellow-500/10 text-yellow-600 dark:text-yellow-400 border-yellow-500/20"
                  : "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border-emerald-500/20"
            }
          `}
        >

          
          {alert.severity}
        </span>
      </div>

      <p className="mt-3 text-sm text-muted-foreground leading-relaxed flex-grow">
        {alert.description}
      </p>

      <div className="mt-4 pt-4 border-t border-border/50">
        <p className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground/60">
          Recommendation:
        </p>
        <ul className="mt-2 space-y-1">
          {alert.recommendation.map((r, i) => (
            <li key={i} className="text-xs text-foreground/70 flex gap-2">
              <span className="text-primary">•</span> {r}
            </li>
          ))}
        </ul>
        <p className="mt-4 text-[10px] font-mono font-bold text-muted-foreground uppercase">
          Region: <span className="text-foreground">{alert.regionName}</span>
        </p>
      </div>
    </div>
  );
}
