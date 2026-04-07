export default function AnalysisHeader() {
  
  return (
    <div className="flex flex-col justify-center border-none outline-none">
      <h1 className="text-2xl font-black tracking-tight text-foreground m-0 p-0">
        Map Analysis
      </h1>
      <p className="text-xs sm:text-sm text-muted-foreground font-medium m-0 p-0">
        Pick coordinates, choose years, and submit for change detection
      </p>
    </div>
  );
}
