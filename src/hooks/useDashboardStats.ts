"use client";

import { useMemo } from "react";
import type { DashboardStats } from "@/types/analysis";
import { useJobHistory } from "@/hooks/useJobHistory";
import { computeDashboardStats } from "@/lib/jobs/metrics";


export function useDashboardStats() {
  const { history } = useJobHistory();

  const data: DashboardStats = useMemo(
    () => computeDashboardStats(history),
    [history],
  );

  return { data, loading: false };
}
