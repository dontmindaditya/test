"use client";

import { useEffect, useState } from "react";
import type { Alert, AlertSeverity } from "@/types/alert";
import { generateAlertsFromJobs } from "@/lib/alerts/alertGenerator";
import { getJobHistory } from "@/lib/jobs/jobStorage";

export function useAlerts() {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [loading, setLoading] = useState(true);
  const [severity, setSeverity] = useState<"ALL" | AlertSeverity>("ALL");

  
  useEffect(() => {
    const refresh = () => {
      setLoading(true);
      const jobs = getJobHistory();
      const generated = generateAlertsFromJobs(jobs);
      setAlerts(Array.isArray(generated) ? generated : []);
      setLoading(false);
    };

    refresh();
    window.addEventListener("job-history-updated", refresh);
    window.addEventListener("storage", refresh);

    return () => {
      window.removeEventListener("job-history-updated", refresh);
      window.removeEventListener("storage", refresh);
    };
  }, []);

  const filtered =
    severity === "ALL" ? alerts : alerts.filter((a) => a.severity === severity);

  return {
    alerts: filtered,
    total: filtered.length,
    loading,
    severity,
    setSeverity,
  };
}

