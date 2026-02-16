"use client";


import { useState } from "react";
import type { AnalyzeRequest, AnalyzeResponse } from "@/types/jobs";
import { submitAnalysis } from "@/lib/api/analyzeService";
import { upsertJob } from "@/lib/jobs/jobStorage";

export function useAnalyzeRegion() {
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<AnalyzeResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  const run = async (payload: AnalyzeRequest) => {
    console.log("useAnalyzeRegion: Starting analysis with payload:", payload);
    
    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const res = await submitAnalysis(payload);
      
      console.log("useAnalyzeRegion: Analysis successful:", res);
      setResult(res);
      upsertJob({
        job_id: res.job_id,
        status: res.status,
        progress: res.status === "Completed" ? 100 : 0,
        message: res.message || "",
        coordinates: payload.coordinates,
        start_year: payload.start_year,
        end_year: payload.end_year,
        change_types: payload.change_types,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      });
      return res;
    } catch (e: any) {
      console.error("useAnalyzeRegion: Analysis failed:", e);
      const errorMessage = e.message || "Something went wrong during analysis";
      setError(errorMessage);
      throw new Error(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  const reset = () => {
    setLoading(false);
    setResult(null);
    setError(null);
  };

  return { run, loading, result, error, reset };
}
