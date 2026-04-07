"use client";
import {
  Target,Satellite,Leaf,
  Search,
  FileText,
  Loader2,
  Upload,
  Cpu,
} from "lucide-react";


// Define the type inline instead of importing
interface AnalysisProgress {
  stage: string;
  message: string;
}

interface ScanProgressLogProps {
  loading: boolean;
  result: any;
  progress?: AnalysisProgress;
}

export default function ScanProgressLog({
  loading,
  result,
  progress,
}: ScanProgressLogProps) {
  const steps = [
    { id: 1, label: "AOI VALIDATION", icon: Target, stage: "initializing" },
    { id: 2, label: "SATELLITE ACQUISITION", icon: Satellite, stage: "initializing" },
    { id: 3, label: "FILE UPLOAD", icon: Upload, stage: "uploading" },
    { id: 4, label: "NDVI RADIOMETRY", icon: Leaf, stage: "processing" },
    { id: 5, label: "CHANGE DETECTION", icon: Search, stage: "processing" },
    { id: 6, label: "AI PROCESSING", icon: Cpu, stage: "processing" },
    { id: 7, label: "REPORT ENCRYPTION", icon: FileText, stage: "completed" },
  ];

  const getStepStatus = (stepStage: string) => {
    if (!loading && !result) return "pending";
    if (!loading && result) return "completed";
    
    // Loading state - determine based on progress
    const currentStage = progress?.stage || "initializing";
    
    const stageOrder = ["initializing", "uploading", "processing", "completed"];
    const currentIndex = stageOrder.indexOf(currentStage);
    const stepIndex = stageOrder.indexOf(stepStage);
    
    if (stepIndex < currentIndex) return "completed";
    if (stepIndex === currentIndex) return "processing";
    return "pending";
  };

  return (
    <div className="rounded-2xl border border-border bg-card p-6 shadow-sm">
      <p className="text-[10px] font-black uppercase tracking-[0.2em] text-muted-foreground mb-6">
        System Diagnostics
      </p>
      
      {/* Progress Message */}
      {loading && progress?.message && (
        <div className="mb-4 p-3 bg-primary/5 border border-primary/10 rounded-lg">
          <p className="text-xs text-primary font-medium flex items-center gap-2">
            <Loader2 size={12} className="animate-spin" />
            {progress.message}
          </p>
        </div>
      )}
      
      <div className="space-y-4">
        {steps.map((step) => {
          const status = getStepStatus(step.stage);
          return (
            <div key={step.id} className="flex items-center gap-4">
              <div
                className={`w-8 h-8 rounded-lg flex items-center justify-center border transition-all ${
                  status === "completed"
                    ? "bg-emerald-500/10 border-emerald-500/20 text-emerald-600 dark:text-emerald-400"
                    : status === "processing"
                      ? "bg-primary/10 border-primary/20 text-primary animate-pulse"
                      : "bg-muted border-border text-muted-foreground/30"
                }`}
              >
                {status === "processing" ? (
                  <Loader2 size={14} className="animate-spin" />
                ) : (
                  <step.icon size={14} />
                )}
              </div>
              <div className="flex-1">
                <p
                  className={`text-[10px] font-black tracking-widest ${status === "pending" ? "text-muted-foreground/30" : "text-foreground"}`}
                >
                  {step.label}
                </p>
                <div className="h-[2px] w-full bg-muted mt-1 rounded-full overflow-hidden">
                  <div
                    className={`h-full transition-all duration-1000 ${status === "completed" ? "w-full bg-emerald-500" : status === "processing" ? "w-1/2 bg-primary" : "w-0"}`}
                  />
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
