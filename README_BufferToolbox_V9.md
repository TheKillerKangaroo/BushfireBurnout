# Bushfire Preliminary Assessment – BufferToolbox_V9.pyt

This ArcPy Python toolbox (`BufferToolbox_V9.pyt`) implements a semi‑automated “bushfire preliminary assessment” workflow for a single project area.

It:

- Buffers a selected project (“subject site”) from a feature service.
- Clips 2 m contours and SVTM vegetation polygons to that buffer.
- Builds a TIN from the 2 m contours.
- Buffers building outlines and computes SVTM inside that building buffer, and SVTM outside the building footprint.
- Converts the TIN to a 1 m DSM raster, then polygonises it.
- Splits DSM polygons into **Above** vs **Below/Equal** a threshold elevation.
- Optionally overwrites existing outputs and adds them to the current map.
- Narrates its progress in a Monty‑Python‑inspired tone.

Note: **Slope statistics have been intentionally removed** from this version to avoid unstable `ZonalStatisticsAsTable` behaviour.

---

## 1. Requirements

- ArcGIS Pro (3.x recommended).
- Licenses:
  - Spatial Analyst
  - 3D Analyst
- A file geodatabase workspace:
  - Example:  
    `D:\GIS Data\ArcGIS Projects\Bushfire Automation\Bushfire Automation.gdb`
- A “Project Study Area” feature service at:

  ```text
  https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/Project_Study_Area/FeatureServer/0
  ```

  The service must have a `project_number` field matching your project IDs.

- A 2 m contour layer and a building outline layer accessible in the current map / catalog.

---

## 2. Installation

1. Save the script as:

   ```text
   BufferToolbox_V9.pyt
   ```

2. In ArcGIS Pro:
   - Open your project.
   - In the **Catalog** pane, right‑click **Toolboxes** → **Add Toolbox**.
   - Browse to `BufferToolbox_V9.pyt` and add it.

3. You should see a single tool:

   - **Bushfire Preliminary Assessment** (class: `SiteBufferToolV9`).

If you change the `.pyt`, remove and re‑add it in Pro (or restart Pro) so parameter definitions refresh.

---

## 3. Parameters

The tool parameters (in order) are:

1. **Output Workspace (GDB)** – *Required*

   - File geodatabase where all outputs will be stored.
   - Example:  
     `D:\GIS Data\ArcGIS Projects\Bushfire Automation\Bushfire Automation.gdb`

2. **Project Number** – *Required*

   - Populated from the `Project_Study_Area` feature service’s `project_number` values.
   - Pick the project you want to process (e.g. `6767`).

3. **Site Buffer Distance (meters)** – *Required*

   - Distance to buffer the project polygon.
   - Default: `200`.

4. **2m Contour Feature Class (or layer)** – *Required*

   - 2 m contours used to build the TIN and DSM.
   - May be a layer in the map or a feature class path.
   - Example: `Gosford-CONT-AHD_56_2m`.

5. **Building Outline Feature Class (or layer)** – *Required*

   - Polygon building footprints (must be polygon geometry).
   - Example: `Buildings`.

6. **Building Buffer Distance (meters)** – *Required*

   - Distance to buffer the building outlines.
   - Default: `140`.

7. **Elevation Threshold (meters)** – *Required*

   - Threshold used to separate DSM polygons into:
     - **Greater**: `Elevation > threshold`
     - **LessEqual**: `Elevation <= threshold`
   - Example: `75.2`.

8. **Overwrite existing outputs** – *Optional (Boolean)*

   - If **True**:
     - The tool searches the workspace (and feature datasets) for any existing objects with the target output names and deletes them before writing new ones.
   - If **False**:
     - Existing objects are **renamed** with a date suffix, e.g.  
       `AEP6767_DSM_1m` → `AEP6767_DSM_1m_20251211`.

9. **Add outputs to current map** – *Optional (Boolean)*

   - If **True**, adds final outputs to the active map at the end.
   - If **False**, outputs are just written to the geodatabase.

---

## 4. Outputs

All feature classes are created in a feature dataset:

```text
<workspace>\BufferLayers_EPSG8058
```

in `GDA2020_NSW_Lambert` (EPSG 8058). The main outputs are:

- **Site buffer**

  - `Site_Buffer_<distance>`  
    e.g. `Site_Buffer_200`

- **Contours (clipped)**

  - `AEP<project>2m_Contours`  
    e.g. `AEP67672m_Contours`

- **SVTM (site buffer)**

  - `AEP<project>_SVTM_<yyyymmdd>`  
    e.g. `AEP6767_SVTM_20251211`

- **TIN**

  - Stored in a `TINs` folder **next to** the GDB:
    - `<project root>\TINs\AEP<project>_TIN`  
      e.g. `D:\...\TINs\AEP6767_TIN`

- **Building buffer**

  - `ARP<project>_Building_Buffer_<distance>M`  
    e.g. `ARP6767_Building_Buffer_140M`

- **SVTM within building buffer**

  - `AEP<project>_SVTM_Bld_Buffer_<yyyymmdd>`  
    e.g. `AEP6767_SVTM_Bld_Buffer_20251211`

- **SVTM within building buffer, minus buildings**

  - `AEP<project>_SVTM_Bld_Buffer_NoBld_<yyyymmdd>`  
    e.g. `AEP6767_SVTM_Bld_Buffer_NoBld_20251211`

- **DSM raster (1 m)**

  - Stored at the root of the GDB:
    - `AEP<project>_DSM_1m`  
      e.g. `AEP6767_DSM_1m`

- **DSM polygons (1 m)**

  - `AEP<project>_DSM_1m_Polys`  
    e.g. `AEP6767_DSM_1m_Polys`  
  - Contains:
    - `gridcode` (integer cell value)
    - `Elevation` (double, copied from `gridcode`)

- **DSM Above/Below polygons**

  - `AEP<project>_DSM_AboveBelow_<yyyymmdd>`  
    e.g. `AEP6767_DSM_AboveBelow_20251211`  
  - Contains a `Relation` field:
    - `"Greater"` for polygons where `Elevation > threshold`
    - `"LessEqual"` for polygons where `Elevation <= threshold`

---

## 5. What the tool does (step‑by‑step)

1. **Ensure feature dataset**

   - Creates or reuses:
     - `<workspace>\BufferLayers_EPSG8058` in EPSG 8058.

2. **Select project site**

   - From `Project_Study_Area` feature service, by `project_number`.

3. **Site buffer**

   - Buffers the project polygon by the specified distance.

4. **Clip contours**

   - Clips the 2 m contours to the site buffer.

5. **Clip SVTM**

   - Clips the SVTM map service layer to the site buffer.

6. **Build TIN**

   - Uses the clipped contours plus an inferred elevation field (e.g. `Elevation`) to build a TIN.

7. **Building buffer & SVTM around buildings**

   - Buffers the building footprints.
   - Clips the SVTM (site‑buffered) polygons to the building buffer.
   - Erases the building footprint from that clipped SVTM layer to produce **SVTM within building buffer but outside buildings**.

8. **DSM from TIN**

   - Converts the TIN to a 1 m DSM raster, masked and clipped to the site buffer, stored in the GDB root.

9. **DSM polygons and Elevation field**

   - Converts DSM integer raster to polygons.
   - Adds `Elevation` (double) and copies from `gridcode`.

10. **Above/Below threshold**

    - Selects:
      - `Elevation > threshold` → dissolved **Greater** polygons.
      - `Elevation <= threshold` → dissolved **LessEqual** polygons.
    - Merges those into the final `DSM_AboveBelow` FC with a `Relation` attribute.

11. **Map add (optional)**

    - Adds a set of key outputs to the active map, if requested.

---

## 6. Behaviour of “Overwrite existing outputs”

- **True**:
  - The tool deletes any existing dataset anywhere in the GDB (and its feature datasets) with the same name *before* creating the new output.
  - This includes rasters, feature classes, and feature classes inside feature datasets.
- **False**:
  - If an output name already exists, it is renamed with a date suffix, e.g.:
    - `AEP6767_DSM_1m` → `AEP6767_DSM_1m_20251211`
  - The new output is then created with the original name.

---

## 7. Logging and “Monty Python” messages

All messages are prefixed with `[MontyGIS]` (or `[MontyGIS – Warning]`) and are deliberately whimsical, e.g.:

- “TIN successfully created. You may now pretend to be King Arthur of the Triangulated Realm.”
- “Separating terrain into 'Higher than 75.2' and 'Not Quite So High'.”
- “Global purge complete. Bring out the next dataset!”

They are cosmetic only and do not affect functionality.

If you ever want to “de‑Monty” the tool, you can safely:

- Replace `_msg` and `_warn` bodies with plain `arcpy.AddMessage` / `AddWarning`, or
- Strip out the commentary strings.

---

## 8. Notes & Tips

- The building layer **must** be polygon geometry.
- All outputs (except the TIN and DSM raster) live inside the `BufferLayers_EPSG8058` feature dataset, so you can collapse/expand a single node in Catalog to see them.
- If you see “Parameters need repair” in the tool dialog after editing the `.pyt`:
  - Remove the toolbox from the project.
  - Save the `.pyt`.
  - Add it back again (or restart ArcGIS Pro).

---

## 9. Changelog (high level)

- **V9 (current)**:
  - Added **overwrite toggle**.
  - Forced feature dataset SR to **EPSG 8058**.
  - Built TIN, DSM, DSM polygons, and Above/Below polygons into a consistent naming scheme.
  - **Removed slope statistics entirely** due to unreliable `ZonalStatisticsAsTable` behaviour in some environments.
  - Added Monty‑Python‑style messaging for better (and sillier) diagnostics.
