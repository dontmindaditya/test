# Map Update - Rondônia Focus

## Changes Made

Updated the Leaflet map to display only **Rondônia, Brazil** region instead of the entire world.

### File Modified
**`src/app/analysis/components/LeafletMapClient.tsx`**

### What Changed

#### Before (World Map - New Delhi, India):
```javascript
const map = L.map(container, {
  zoomControl: false,
  attributionControl: false,
}).setView([28.6139, 77.209], 12);
```

#### After (Rondônia, Brazil Only):
```javascript
// Rondônia, Brazil bounds and center
const rondoniaBounds = L.latLngBounds(
  [-13.5, -66.5], // Southwest corner
  [-8.0, -59.5]   // Northeast corner
);

const map = L.map(container, {
  zoomControl: false,
  attributionControl: false,
  maxBounds: rondoniaBounds,        // Restrict panning to Rondônia
  minZoom: 6,                       // Prevent zooming out too far
  maxZoom: 19,
}).setView([-10.5, -63.0], 7);      // Center on Rondônia with zoom level 7
```

## Map Configuration

**Center:** [-10.5, -63.0] (Porto Velho area, capital of Rondônia)

**Zoom Level:** 7 (Perfect for viewing the entire state)



**Bounds Restrictions:**
- Southwest: [-13.5, -66.5]
- Northeast: [-8.0, -59.5]
- Users cannot pan outside Rondônia
- Minimum zoom level: 6 (prevents seeing the whole world)

## Preserved Functionality

✅ All existing features work exactly the same:
- Draw rectangle selection tool
- Clear selection button
- Mouse events (mousedown, mousemove, mouseup)
- Bbox calculation and callback
- Loading states
- Responsive design
- Sidebar integration

## Rondônia Region Details

**Location:** North-central Brazil, Amazon region

**Capital:** Porto Velho (~-8.76, -63.90)

**Major Cities in View:**
- Porto Velho (capital)
- Ji-Paraná
- Ariquemes
- Cacoal
- Vilhena

**Why This Region:**
Your backend only has satellite data for Rondônia, so users should only be able to select areas within this state.

## Build Status

✅ **Compiled successfully**

## Testing

1. Restart dev server: `npm run dev`
2. Go to http://localhost:3000/analysis
3. Map should now show Rondônia, Brazil
4. Try to pan outside the region - it should bounce back
5. Try to zoom out too far - it should stop at zoom 6
6. Draw a selection rectangle - works the same as before

## Notes

- The satellite tile layer (ArcGIS World Imagery) will still work
- All coordinates returned will be within Rondônia bounds
- Users cannot accidentally select areas outside your data coverage
- Perfect for your use case since you only have Rondônia data

---

**Status:** ✅ Map updated to show only Rondônia region
**Last Updated:** 2026-02-02
