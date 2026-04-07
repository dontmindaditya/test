"use client";
import { useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import AnalysisHeader from "./components/AnalysisHeader";
import LeafletMapSelector from "./components/LeafletMapSelector";
import { useAnalyzeRegion } from "@/hooks/useAnalyzeRegion";
import { useJobHistory } from "@/hooks/useJobHistory";
import type { ChangeType } from "@/types/jobs";

const YEAR_OPTIONS = [2020, 2021, 2022, 2023, 2024];
const LOCATIONS = [
  { label: "Porto Velho (Capital)", lat: -8.76, lon: -63.9 },
  { label: "Ji-Parana", lat: -10.88, lon: -61.95 },
  { label: "Ariquemes", lat: -9.91, lon: -63.04 },
  { label: "Cacoal", lat: -11.44, lon: -61.45 },
  { label: "Vilhena", lat: -12.74, lon: -60.15 },
  { label: "Pacaas Novos National Park", lat: -10.5, lon: -63.5 },
];

export default function MapAnalysisPage() {
  const router = useRouter();
  const { run, loading, error } = useAnalyzeRegion();
  const { history } = useJobHistory();

  const [coordinates, setCoordinates] = useState<{ lat: number; lon: number }>(
    { lat: -10.0, lon: -63.0 },
  );
  const [mapCenter, setMapCenter] = useState<{ lat: number; lon: number } | null>(
    { lat: -10.0, lon: -63.0 },
  );
  const [startYear, setStartYear] = useState(2021);
  const [endYear, setEndYear] = useState(2024);
  const [changeTypes, setChangeTypes] = useState<ChangeType[]>([
    "deforestation",
    "urban_expansion",
  ]);
  const [selectedLocation, setSelectedLocation] = useState("custom");

  const isValidCoords =
    coordinates.lat >= -90 &&
    coordinates.lat <= 90 &&
    coordinates.lon >= -180 &&
    coordinates.lon <= 180;

  const canSubmit =
    isValidCoords && changeTypes.length > 0 && endYear >= startYear;

  const recentJobs = useMemo(() => history.slice(0, 6), [history]);

  const toggleChangeType = (value: ChangeType) => {
    setChangeTypes((prev) =>
      prev.includes(value)
        ? prev.filter((item) => item !== value)
        : [...prev, value],
    );
  };

  const handleSubmit = async () => {
    if (!canSubmit || loading) return;
    const res = await run({
      coordinates,
      start_year: startYear,
      end_year: endYear,
      change_types: changeTypes,
    });
    router.push(`/scan-result?job_id=${res.job_id}`);
  };

  return (
    <div className="min-h-screen bg-background text-foreground transition-colors duration-300">
      <header className="sticky top-[80px] z-[50] bg-background/80 backdrop-blur-md border-b border-border/50">
        <div className="container mx-auto px-6 py-4">
          <AnalysisHeader />
        </div>
      </header>

      <main className="container mx-auto px-6 py-8">
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-8 items-start">
          <div className="lg:col-span-8 space-y-6">
            <Card className="p-4 rounded-2xl border border-border shadow-sm">
              <div className="flex flex-col gap-3">
                <p className="text-[10px] uppercase tracking-[0.2em] font-bold text-muted-foreground">
                  Popular Locations
                </p>
                <Select
                  value={selectedLocation}
                  onValueChange={(value) => {
                    setSelectedLocation(value);
                    if (value === "custom") return;
                    const match = LOCATIONS.find(
                      (item) => item.label === value,
                    );
                    if (!match) return;
                    const next = { lat: match.lat, lon: match.lon };
                    setCoordinates(next);
                    setMapCenter(next);
                  }}
                >
                  <SelectTrigger className="w-full rounded-xl bg-background">
                    <SelectValue placeholder="Select a location" />
                  </SelectTrigger>
                  <SelectContent>
                    {LOCATIONS.map((loc) => (
                      <SelectItem key={loc.label} value={loc.label}>
                        {loc.label}
                      </SelectItem>
                    ))}
                    <SelectItem value="custom">Custom (map or manual)</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </Card>

            <LeafletMapSelector
              value={coordinates}
              onChange={(next) => {
                setCoordinates(next);
                setMapCenter(next);
                setSelectedLocation("custom");
              }}
              center={mapCenter}
            />

            <Card className="p-6 rounded-2xl border border-border shadow-sm">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
                <div>
                  <p className="text-xs uppercase tracking-[0.2em] font-bold text-muted-foreground">
                    Coordinates
                  </p>
                  <div className="mt-3 grid grid-cols-2 gap-3">
                    <Input
                      value={coordinates.lat}
                      onChange={(e) => {
                        const lat = Number(e.target.value);
                        const next = { ...coordinates, lat };
                        setCoordinates(next);
                        setMapCenter(next);
                        setSelectedLocation("custom");
                      }}
                      type="number"
                      step="0.0001"
                      placeholder="Latitude"
                    />
                    <Input
                      value={coordinates.lon}
                      onChange={(e) => {
                        const lon = Number(e.target.value);
                        const next = { ...coordinates, lon };
                        setCoordinates(next);
                        setMapCenter(next);
                        setSelectedLocation("custom");
                      }}
                      type="number"
                      step="0.0001"
                      placeholder="Longitude"
                    />
                  </div>
                  <p className="mt-3 text-xs text-muted-foreground">
                    Not sure where to start? Select a popular location above or
                    click anywhere on the map. Try Porto Velho (-8.76, -63.90)
                    or Pacaas Novos Park (-10.50, -63.50).
                  </p>
                  {!isValidCoords && (
                    <p className="text-xs text-red-500 mt-2">
                      Please enter valid latitude/longitude values.
                    </p>
                  )}
                </div>

                <div>
                  <p className="text-xs uppercase tracking-[0.2em] font-bold text-muted-foreground">
                    Date Range
                  </p>
                  <div className="mt-3 grid grid-cols-2 gap-3">
                    <Select
                      value={String(startYear)}
                      onValueChange={(value) =>
                        setStartYear(Number(value))
                      }
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Start year" />
                      </SelectTrigger>
                      <SelectContent>
                        {YEAR_OPTIONS.map((year) => (
                          <SelectItem key={year} value={String(year)}>
                            {year}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <Select
                      value={String(endYear)}
                      onValueChange={(value) => setEndYear(Number(value))}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="End year" />
                      </SelectTrigger>
                      <SelectContent>
                        {YEAR_OPTIONS.map((year) => (
                          <SelectItem key={year} value={String(year)}>
                            {year}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  {endYear < startYear && (
                    <p className="text-xs text-red-500 mt-2">
                      End year must be greater than or equal to start year.
                    </p>
                  )}
                </div>
              </div>

              <div className="mt-6">
                <p className="text-xs uppercase tracking-[0.2em] font-bold text-muted-foreground">
                  Change Types
                </p>
                <div className="mt-3 flex flex-wrap gap-4 text-sm">
                  {(
                    [
                      { label: "Deforestation", value: "deforestation" },
                      { label: "Urban Expansion", value: "urban_expansion" },
                      { label: "Encroachment", value: "encroachment" },
                    ] as const
                  ).map((item) => (
                    <label
                      key={item.value}
                      className="flex items-center gap-2 rounded-full border border-border px-4 py-2 bg-muted/30 cursor-pointer"
                    >
                      <input
                        type="checkbox"
                        checked={changeTypes.includes(item.value)}
                        onChange={() => toggleChangeType(item.value)}
                        className="accent-primary"
                      />
                      <span>{item.label}</span>
                    </label>
                  ))}
                </div>
              </div>

              <div className="mt-6 flex flex-col md:flex-row gap-4 items-center justify-between">
                <div className="text-xs text-muted-foreground">
                  Submit to backend job queue. Results will appear once the job
                  completes.
                </div>
                <Button
                  disabled={!canSubmit || loading}
                  onClick={handleSubmit}
                  className="px-6"
                >
                  {loading ? "Submitting..." : "Submit Analysis"}
                </Button>
              </div>

              {error && (
                <div className="mt-4 text-sm text-red-500 font-medium">
                  {error}
                </div>
              )}
            </Card>
          </div>

          <aside className="lg:col-span-4 space-y-6">
            <Card className="p-5 rounded-2xl border border-border shadow-sm">
              <p className="text-[10px] uppercase tracking-[0.2em] font-bold text-muted-foreground">
                Job History
              </p>
              <div className="mt-4 space-y-3">
                {recentJobs.length === 0 ? (
                  <p className="text-xs text-muted-foreground">
                    No jobs yet. Run your first analysis.
                  </p>
                ) : (
                  recentJobs.map((job) => (
                    <button
                      key={job.job_id}
                      onClick={() =>
                        router.push(`/scan-result?job_id=${job.job_id}`)
                      }
                      className="w-full text-left rounded-xl border border-border bg-muted/30 px-4 py-3 hover:bg-muted/50 transition"
                    >
                      <div className="flex items-center justify-between">
                        <span className="text-xs font-mono font-semibold text-primary">
                          {job.job_id.slice(0, 10)}...
                        </span>
                        <span className="text-[10px] uppercase tracking-widest text-muted-foreground">
                          {job.status}
                        </span>
                      </div>
                      <div className="mt-2 text-xs text-muted-foreground">
                        {job.coordinates.lat.toFixed(2)},{" "}
                        {job.coordinates.lon.toFixed(2)} • {job.start_year}→
                        {job.end_year}
                      </div>
                    </button>
                  ))
                )}
              </div>
            </Card>
          </aside>
        </div>
      </main>
    </div>
  );
}
