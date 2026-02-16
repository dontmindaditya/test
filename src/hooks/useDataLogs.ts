"use client";


import { useEffect, useMemo, useState } from "react";
import type { DataLogsFilters } from "@/types/dataLogs";
import { getFilteredLogs } from "@/lib/data-logs/dataLogsService";
import type { JobHistoryItem } from "@/types/jobs";
import { clearJobHistory } from "@/lib/jobs/jobStorage";

export function useDataLogs() {
  const [filters, setFilters] = useState<DataLogsFilters>({
    query: "",
    status: "ALL",
    sort: "NEWEST",
  });

  const [logs, setLogs] = useState<JobHistoryItem[]>([]);

  const refresh = () => {
    const data = getFilteredLogs(filters);
    setLogs(data);
  };

  useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filters.query, filters.status, filters.sort]);

  const total = useMemo(() => logs.length, [logs]);

  const clearAll = () => {
    clearJobHistory();
    setLogs([]);
  };

  return {
    logs,
    total,
    filters,
    setFilters,
    refresh,
    clearAll,
  };
}

