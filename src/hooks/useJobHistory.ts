"use client";


import { useEffect, useMemo, useState } from "react";
import type { JobHistoryItem } from "@/types/jobs";
import { clearJobHistory, getJobHistory } from "@/lib/jobs/jobStorage";

export function useJobHistory() {
  const [history, setHistory] = useState<JobHistoryItem[]>([]);
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);

  useEffect(() => {
    const items = getJobHistory();
    setHistory(items);
    if (items.length > 0) setSelectedJobId(items[0].job_id);

    const onUpdate = () => {
      const next = getJobHistory();
      setHistory(next);
      if (next.length > 0 && !selectedJobId) {
        setSelectedJobId(next[0].job_id);
      }
    };

    window.addEventListener("job-history-updated", onUpdate);
    window.addEventListener("storage", onUpdate);
    return () => {
      window.removeEventListener("job-history-updated", onUpdate);
      window.removeEventListener("storage", onUpdate);
    };
  }, []);

  const selectedJob = useMemo(() => {
    if (!selectedJobId) return null;
    return history.find((j) => j.job_id === selectedJobId) ?? null;
  }, [history, selectedJobId]);

  const refresh = () => {
    const items = getJobHistory();
    setHistory(items);
    if (items.length > 0 && !selectedJobId) setSelectedJobId(items[0].job_id);
  };

  const clearAll = () => {
    clearJobHistory();
    setHistory([]);
    setSelectedJobId(null);
  };

  return {
    history,
    selectedJob,
    selectedJobId,
    setSelectedJobId,
    refresh,
    clearAll,
  };
}
