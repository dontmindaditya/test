"use client";

import dynamic from "next/dynamic";

const LeafletMapSelectorClient = dynamic(
  () => import("./LeafletMapSelectorClient"),
  { ssr: false },
);


export default function LeafletMapSelector(props: {
  value: { lat: number; lon: number } | null;
  onChange: (value: { lat: number; lon: number }) => void;
  center?: { lat: number; lon: number } | null;
  height?: number;
}) {
  return <LeafletMapSelectorClient {...props} />;
}
