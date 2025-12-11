import arcpy
from arcpy import env
from arcpy.sa import *
import os
import math
import uuid
import tempfile
import traceback
import re

class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the .pyt file)."""
        self.label = "Slope Analysis Toolbox"
        self.alias = "slope_analysis"
        self.tools = [SlopeByPolygon]

class SlopeByPolygon(object):
    def __init__(self):
        self.label = "Slope Analysis by Polygon"
        self.description = ("Convert a TIN to a 1m raster, derive slope and aspect, "
                            "sample raster values into polygons and compute a set of statistics "
                            "for elevation, slope and aspect. Results are written to a new "
                            "feature class in a feature dataset named 'Slope' inside the "
                            "project's default geodatabase (spatial reference EPSG:8058).")
        self.canRunInBackground = False

    def getParameterInfo(self):
        params = []

        # Use String for the TIN parameter to avoid ParameterObject datatype initialization
        # errors across different ArcGIS Pro builds. The tool supports selecting a TIN layer
        # from the current map (ArcGIS will pass a CIMPATH-like value), and that is resolved
        # at runtime by _resolve_tin_input().
        tin = arcpy.Parameter(
            displayName="Input TIN (choose from map or enter path)",
            name="in_tin",
            datatype="String",
            parameterType="Required",
            direction="Input"
        )
        params.append(tin)

        polygons = arcpy.Parameter(
            displayName="Input Polygon Feature Class",
            name="in_polygons",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )
        params.append(polygons)

        add_to_map = arcpy.Parameter(
            displayName="Add output to current map",
            name="add_to_map",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input"
        )
        add_to_map.value = True
        params.append(add_to_map)

        return params

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def _calculate_polygon_area(self, fc, area_field, messages):
        """
        Calculate polygon area (in m^2) into area_field for feature class fc.
        Tries geodesic AREA_GEODESIC first, then AREA, then a few alternate unit strings
        for compatibility with different ArcGIS builds. Raises if none succeed.
        """
        # Ensure field exists
        if area_field not in [f.name for f in arcpy.ListFields(fc)]:
            arcpy.AddField_management(fc, area_field, "DOUBLE")

        # Try geodesic area first
        tried = []
        try:
            messages.addMessage("Calculating polygon area using geodesic AREA_GEODESIC (Square Meters)...")
            arcpy.management.CalculateGeometryAttributes(fc, [[area_field, "AREA_GEODESIC"]], area_unit="Square Meters")
            messages.addMessage("Calculated geodesic area into field '{}'.".format(area_field))
            return
        except Exception as e:
            tried.append(("AREA_GEODESIC", "Square Meters", str(e)))
            # continue to fallback attempts

        # Try planar AREA with standard human-readable unit
        try:
            messages.addMessage("Falling back to planar AREA (Square Meters)...")
            arcpy.management.CalculateGeometryAttributes(fc, [[area_field, "AREA"]], area_unit="Square Meters")
            messages.addMessage("Calculated planar area into field '{}'.".format(area_field))
            return
        except Exception as e:
            tried.append(("AREA", "Square Meters", str(e)))

        # Try a few alternate unit strings for compatibility (in case some builds accept different tokens)
        alt_units = ["Square Meters", "SquareMeters", "SQUARE_METERS", "SQUAREMETERS", "Square Meters"]
        for alt in alt_units:
            try:
                messages.addMessage(f"Attempting CalculateGeometryAttributes with area_unit='{alt}'...")
                arcpy.management.CalculateGeometryAttributes(fc, [[area_field, "AREA"]], area_unit=alt)
                messages.addMessage(f"Calculated area using unit token '{alt}' into field '{area_field}'.")
                return
            except Exception as e:
                tried.append(("AREA", alt, str(e)))
                continue

        # If we reach here, none of the attempts worked — build an informative error message
        msg_lines = ["Failed to calculate polygon areas; attempted the following method(s):"]
        for m, u, err in tried:
            msg_lines.append(f" - method: {m}, area_unit: {u}, error: {err}")
        msg_lines.append("CalculateGeometryAttributes failed for all attempted options.")
        raise arcpy.ExecuteError("\n".join(msg_lines))

    def _resolve_tin_input(self, tin_param_value, messages):
        """
        Resolve the TIN input whether it's a map layer reference or a direct dataset path.
        Returns a path to the underlying TIN dataset suitable for arcpy tools.
        """
        # If the value directly exists as a dataset, return it
        if tin_param_value and arcpy.Exists(tin_param_value):
            return tin_param_value

        # Handle layer references coming from the map (CIMPATH or similar strings)
        # Try to extract a likely layer name and match it to layers in the current project maps.
        aprx = None
        try:
            aprx = arcpy.mp.ArcGISProject("CURRENT")
        except Exception:
            aprx = None

        candidate_name = None
        v = tin_param_value or ""
        if "CIMPATH=" in v:
            # Example: "CIMPATH=Map/AEP6767_TIN.json" -> take "AEP6767_TIN"
            tail = v.split("=", 1)[-1]
            tail = tail.split("/")[-1]
            if tail.lower().endswith(".json"):
                tail = tail[:-5]
            candidate_name = tail
        else:
            # Could be a plain layer name or path; try to use it as a name
            candidate_name = v

        if aprx:
            for m in aprx.listMaps():
                for lyr in m.listLayers():
                    # Compare several ways: exact name, endswith, longName, and layer's canonical name
                    try:
                        if lyr.name == candidate_name:
                            messages.addMessage(f"Resolved TIN layer by exact name: {lyr.name}")
                            return lyr.dataSource if lyr.supports("dataSource") else None
                        if candidate_name.endswith(lyr.name):
                            messages.addMessage(f"Resolved TIN layer by name-end match: {lyr.name}")
                            return lyr.dataSource if lyr.supports("dataSource") else None
                        if lyr.longName and lyr.longName == candidate_name:
                            messages.addMessage(f"Resolved TIN layer by longName: {lyr.longName}")
                            return lyr.dataSource if lyr.supports("dataSource") else None
                    except Exception:
                        # some layer types may not expose dataSource; skip them
                        continue

        # As a last attempt, if the value looks like a layer name (no path separators) try to find a layer matching that name across maps
        if aprx and candidate_name:
            for m in aprx.listMaps():
                for lyr in m.listLayers():
                    if lyr.name.lower() == candidate_name.lower():
                        if lyr.supports("dataSource"):
                            messages.addMessage(f"Resolved TIN layer by case-insensitive name: {lyr.name}")
                            return lyr.dataSource
                        else:
                            continue

        # Could not resolve to an existing dataset
        return None

    def _rename_field_like(self, fc, orig_prefix, newname):
        """
        Rename the first field in fc that starts with orig_prefix (case-insensitive) to newname.
        Returns True if rename performed, False otherwise.
        """
        fields = [f.name for f in arcpy.ListFields(fc)]
        for fname in fields:
            if fname.lower().startswith(orig_prefix.lower()):
                # don't try to rename if the target already exists
                if newname in fields:
                    return False
                try:
                    arcpy.management.AlterField(fc, fname, new_field_name=newname, new_field_alias=newname)
                    return True
                except Exception:
                    # ignore and continue to try other matches
                    continue
        return False

    def execute(self, parameters, messages):
        arcpy.env.overwriteOutput = True

        raw_tin_param = parameters[0].valueAsText
        in_polygons = parameters[1].valueAsText
        add_to_map = bool(parameters[2].value) if parameters[2].value is not None else False

        messages.addMessage(f"Raw TIN parameter value: {raw_tin_param}")

        # Resolve TIN input to an actual dataset path (supports map layer selection)
        in_tin = None
        # If parameter is a layer object, valueAsText may be a string; try to resolve
        if raw_tin_param:
            # If path exists on disk / geodatabase as-is, use it
            if arcpy.Exists(raw_tin_param):
                in_tin = raw_tin_param
            else:
                # Try to resolve a map layer reference to its data source
                resolved = self._resolve_tin_input(raw_tin_param, messages)
                if resolved and arcpy.Exists(resolved):
                    in_tin = resolved
                else:
                    # As fallback, try using the raw value directly if Describe works
                    try:
                        desc_try = arcpy.Describe(raw_tin_param)
                        if desc_try:
                            in_tin = raw_tin_param
                    except Exception:
                        in_tin = None

        if not in_tin:
            raise arcpy.ExecuteError("Unable to resolve the Input TIN parameter to an existing TIN dataset. "
                                     "If you selected a layer from the map, ensure it points to a valid TIN. "
                                     "Resolved value: {}".format(raw_tin_param))

        # Validate that the provided input is a TIN (Describe-based check)
        try:
            desc = arcpy.Describe(in_tin)
            ds_type = getattr(desc, "datasetType", None) or getattr(desc, "dataType", None) or ""
            if ds_type is None or "tin" not in str(ds_type).lower():
                raise arcpy.ExecuteError(f"The input provided for 'Input TIN' does not appear to be a TIN. Describe datasetType/dataType = '{ds_type}'. Please choose a TIN dataset.")
        except arcpy.ExecuteError:
            raise
        except Exception as e:
            raise arcpy.ExecuteError(f"Failed to validate TIN input: {e}")

        # 1m cell size, target SR EPSG:8058
        cell_size = 1
        target_sr = arcpy.SpatialReference(8058)

        # Get default project geodatabase and aprx for potential map additions
        aprx = None
        try:
            aprx = arcpy.mp.ArcGISProject("CURRENT")
            default_gdb = aprx.defaultGeodatabase
        except Exception:
            # Fall back to workspace or raise
            default_gdb = arcpy.env.workspace
            if not default_gdb:
                raise arcpy.ExecuteError("Unable to determine the project's default geodatabase. Open a project or set arcpy.env.workspace.")

        messages.addMessage(f"Using default geodatabase: {default_gdb}")

        # Create Slope feature dataset if not exists
        fd_name = "Slope"
        fd_path = os.path.join(default_gdb, fd_name)
        if not arcpy.Exists(fd_path):
            messages.addMessage(f"Creating feature dataset '{fd_name}' with spatial reference EPSG:8058")
            arcpy.CreateFeatureDataset_management(default_gdb, fd_name, target_sr)
        else:
            messages.addMessage(f"Feature dataset '{fd_name}' already exists")

        # Output feature class name
        in_polygons_name = arcpy.Describe(in_polygons).baseName
        out_fc_name = f"{in_polygons_name}_Slope"
        out_fc = os.path.join(fd_path, out_fc_name)

        # Make a temp workspace prefix
        guid = uuid.uuid4().hex[:8]
        tmp_ws = arcpy.env.scratchGDB or arcpy.env.scratchFolder or tempfile.gettempdir()
        mem = "in_memory"

        # 1) Convert TIN to a raster (elevation)
        messages.addMessage("Converting TIN to raster (1 m cell size)...")
        tin_rast = os.path.join(mem, f"tin_rast_{guid}")
        try:
            arcpy.ddd.TinRaster(in_tin, tin_rast, "FLOAT", "#", "CELLSIZE", str(cell_size))
        except Exception:
            tin_rast = os.path.join(default_gdb, f"tin_rast_{guid}")
            arcpy.ddd.TinRaster(in_tin, tin_rast, "FLOAT", "#", "CELLSIZE", str(cell_size))
        messages.addMessage(f"TIN raster created: {tin_rast}")

        # 2) Derive slope and aspect rasters (degrees)
        messages.addMessage("Deriving slope raster (degrees)...")
        slope_rast = Slope(tin_rast, "DEGREE", z_factor=1)
        messages.addMessage("Deriving aspect raster (degrees)...")
        aspect_rast = Aspect(tin_rast)

        # 3) Prepare polygon copy in feature dataset and add ZoneID
        messages.addMessage("Preparing output polygon feature class and zone ID field...")
        if arcpy.Exists(out_fc):
            messages.addMessage(f"Output feature class {out_fc} already exists, it will be overwritten.")
            arcpy.Delete_management(out_fc)

        arcpy.management.CopyFeatures(in_polygons, out_fc)
        zone_field = "ZoneID"
        if zone_field in [f.name for f in arcpy.ListFields(out_fc)]:
            messages.addMessage(f"Zone field {zone_field} exists; it will be recalculated.")
        else:
            arcpy.AddField_management(out_fc, zone_field, "LONG")

        # Populate ZoneID with the object's FID (OBJECTID)
        oid_field = arcpy.Describe(out_fc).OIDFieldName
        arcpy.management.CalculateField(out_fc, zone_field, f"!{oid_field}!", "PYTHON3")

        # 4) RasterToPoint for elevation, slope, aspect
        messages.addMessage("Converting rasters to point sample features (1m points). This can be heavy for large areas.")
        elev_pts = os.path.join(mem, f"elev_pts_{guid}")
        slope_pts = os.path.join(mem, f"slope_pts_{guid}")
        aspect_pts = os.path.join(mem, f"aspect_pts_{guid}")

        arcpy.conversion.RasterToPoint(tin_rast, elev_pts, "VALUE")
        arcpy.conversion.RasterToPoint(slope_rast, slope_pts, "VALUE")
        arcpy.conversion.RasterToPoint(aspect_rast, aspect_pts, "VALUE")

        # Normalize raster value field to 'VALUE'
        for pts in (elev_pts, slope_pts, aspect_pts):
            val_field = None
            for f in arcpy.ListFields(pts):
                if f.type in ("Integer", "Double", "Single", "SmallInteger", "OID") and f.name.lower() in ("grid_code", "value", "raster_val", "band_1"):
                    val_field = f.name
                    break
            if not val_field:
                fields = [f.name for f in arcpy.ListFields(pts)]
                if len(fields) >= 2:
                    val_field = fields[1]
                else:
                    raise arcpy.ExecuteError(f"Unable to find value field in {pts}")

            if val_field != "VALUE":
                try:
                    arcpy.management.AlterField(pts, val_field, new_field_name="VALUE")
                except Exception:
                    arcpy.AddField_management(pts, "VALUE", "DOUBLE")
                    arcpy.management.CalculateField(pts, "VALUE", f"!{val_field}!", "PYTHON3")

        # 5) Spatial join sample points to polygons so each sample has ZoneID
        messages.addMessage("Spatially joining sample points to polygons to assign Zone IDs to points...")
        elev_pts_z = os.path.join(mem, f"elev_pts_z_{guid}")
        slope_pts_z = os.path.join(mem, f"slope_pts_z_{guid}")
        aspect_pts_z = os.path.join(mem, f"aspect_pts_z_{guid}")

        arcpy.analysis.SpatialJoin(elev_pts, out_fc, elev_pts_z, "JOIN_ONE_TO_ONE", "KEEP_COMMON", match_option="INTERSECT")
        arcpy.analysis.SpatialJoin(slope_pts, out_fc, slope_pts_z, "JOIN_ONE_TO_ONE", "KEEP_COMMON", match_option="INTERSECT")
        arcpy.analysis.SpatialJoin(aspect_pts, out_fc, aspect_pts_z, "JOIN_ONE_TO_ONE", "KEEP_COMMON", match_option="INTERSECT")

        def find_zone_field(fc):
            for f in arcpy.ListFields(fc):
                if f.name.lower().startswith(zone_field.lower()):
                    return f.name
            return None

        zone_field_elev = find_zone_field(elev_pts_z)
        zone_field_slope = find_zone_field(slope_pts_z)
        zone_field_aspect = find_zone_field(aspect_pts_z)

        if not zone_field_elev or not zone_field_slope or not zone_field_aspect:
            raise arcpy.ExecuteError("Failed to find zone ID field after spatial join. Aborting.")

        # 6) Compute statistics via Statistics_analysis
        messages.addMessage("Computing zonal statistics (min, max, mean, std, median, count) using point samples...")
        elev_stats_tbl = os.path.join(mem, f"elev_stats_{guid}")
        slope_stats_tbl = os.path.join(mem, f"slope_stats_{guid}")

        stat_fields = [["VALUE", "MIN"], ["VALUE", "MAX"], ["VALUE", "MEAN"], ["VALUE", "STD"], ["VALUE", "MEDIAN"], ["VALUE", "COUNT"]]

        arcpy.analysis.Statistics(elev_pts_z, elev_stats_tbl, stat_fields, case_field=zone_field_elev)
        arcpy.analysis.Statistics(slope_pts_z, slope_stats_tbl, stat_fields, case_field=zone_field_slope)

        # 7) Aspect circular stats
        messages.addMessage("Computing circular mean and circular standard deviation for aspect values per zone...")
        aspect_stats_tbl = os.path.join(mem, f"aspect_stats_{guid}")
        arcpy.management.CreateTable(mem, f"aspect_stats_{guid}")
        arcpy.AddField_management(aspect_stats_tbl, zone_field, "LONG")
        arcpy.AddField_management(aspect_stats_tbl, "ASPECT_MEAN_DEG", "DOUBLE")
        arcpy.AddField_management(aspect_stats_tbl, "ASPECT_STD_DEG", "DOUBLE")
        arcpy.AddField_management(aspect_stats_tbl, "ASPECT_SAMPLE_COUNT", "LONG")

        sums = {}
        counts = {}
        for row in arcpy.da.SearchCursor(aspect_pts_z, [zone_field_aspect, "VALUE"]):
            z = row[0]
            v = row[1]
            if v is None:
                continue
            try:
                val = float(v)
            except Exception:
                continue
            if val < 0:
                continue
            rad = math.radians(val)
            cs = math.cos(rad)
            sn = math.sin(rad)
            if z in sums:
                sums[z][0] += cs
                sums[z][1] += sn
                counts[z] += 1
            else:
                sums[z] = [cs, sn]
                counts[z] = 1

        with arcpy.da.InsertCursor(aspect_stats_tbl, [zone_field, "ASPECT_MEAN_DEG", "ASPECT_STD_DEG", "ASPECT_SAMPLE_COUNT"]) as icur:
            for z, (sum_cos, sum_sin) in sums.items():
                n = counts.get(z, 0)
                if n == 0:
                    continue
                avg_cos = sum_cos / n
                avg_sin = sum_sin / n
                mean_rad = math.atan2(avg_sin, avg_cos)
                mean_deg = math.degrees(mean_rad)
                if mean_deg < 0:
                    mean_deg += 360.0
                R = math.sqrt(avg_cos * avg_cos + avg_sin * avg_sin)
                try:
                    circ_std = math.sqrt(-2.0 * math.log(max(min(R, 1.0), 1e-12)))
                except Exception:
                    circ_std = 0.0
                circ_std_deg = math.degrees(circ_std)
                icur.insertRow((z, mean_deg, circ_std_deg, n))

        # 8) Slope class percentages via TabulateArea
        messages.addMessage("Computing percent area in slope classes using Tabulate Area...")
        # Define slope class ranges (degrees) -> codes
        # Classes: 0-5, 5-15, 15-30, 30-45, 45-360
        class_definitions = {
            1: "0_5",
            2: "5_15",
            3: "15_30",
            4: "30_45",
            5: "45_plus"
        }
        remap = RemapRange([[0, 5, 1],
                            [5, 15, 2],
                            [15, 30, 3],
                            [30, 45, 4],
                            [45, 360, 5]])
        slope_class_rast = Reclassify(slope_rast, "VALUE", remap, "NODATA")
        class_rast_temp = os.path.join(mem, f"slope_class_{guid}")
        slope_class_rast.save(class_rast_temp)

        tab_area_tbl = os.path.join(mem, f"tab_area_{guid}")
        arcpy.sa.TabulateArea(out_fc, zone_field, class_rast_temp, "VALUE", tab_area_tbl, cell_size)

        # 9) Join statistics back to output feature class
        messages.addMessage("Joining statistics back to output feature class...")
        elev_case_field = [f.name for f in arcpy.ListFields(elev_stats_tbl)][0]
        arcpy.management.JoinField(out_fc, zone_field, elev_stats_tbl, elev_case_field,
                                  ["MIN_VALUE", "MAX_VALUE", "MEAN_VALUE", "STD_VALUE", "MEDIAN_VALUE", "COUNT_VALUE"])

        # Rename elevation stat fields to descriptive names
        # ELEV_MIN_M, ELEV_MAX_M, ELEV_MEAN_M, ELEV_STD_M, ELEV_MEDIAN_M, ELEV_SAMPLE_COUNT
        self._rename_field_like(out_fc, "MIN_VALUE", "ELEV_MIN_M")
        self._rename_field_like(out_fc, "MAX_VALUE", "ELEV_MAX_M")
        self._rename_field_like(out_fc, "MEAN_VALUE", "ELEV_MEAN_M")
        self._rename_field_like(out_fc, "STD_VALUE", "ELEV_STD_M")
        self._rename_field_like(out_fc, "MEDIAN_VALUE", "ELEV_MEDIAN_M")
        self._rename_field_like(out_fc, "COUNT_VALUE", "ELEV_SAMPLE_COUNT")

        # Join slope stats
        slope_case_field = [f.name for f in arcpy.ListFields(slope_stats_tbl)][0]
        arcpy.management.JoinField(out_fc, zone_field, slope_stats_tbl, slope_case_field,
                                  ["MIN_VALUE", "MAX_VALUE", "MEAN_VALUE", "STD_VALUE", "MEDIAN_VALUE", "COUNT_VALUE"])

        # Rename slope stat fields to descriptive names
        # SLOPE_MIN_DEG, SLOPE_MAX_DEG, SLOPE_MEAN_DEG, SLOPE_STD_DEG, SLOPE_MEDIAN_DEG, SLOPE_SAMPLE_COUNT
        self._rename_field_like(out_fc, "MIN_VALUE", "SLOPE_MIN_DEG")
        self._rename_field_like(out_fc, "MAX_VALUE", "SLOPE_MAX_DEG")
        self._rename_field_like(out_fc, "MEAN_VALUE", "SLOPE_MEAN_DEG")
        self._rename_field_like(out_fc, "STD_VALUE", "SLOPE_STD_DEG")
        self._rename_field_like(out_fc, "MEDIAN_VALUE", "SLOPE_MEDIAN_DEG")
        self._rename_field_like(out_fc, "COUNT_VALUE", "SLOPE_SAMPLE_COUNT")

        # Join aspect stats (already prepared with descriptive names)
        arcpy.management.JoinField(out_fc, zone_field, aspect_stats_tbl, zone_field,
                                  ["ASPECT_MEAN_DEG", "ASPECT_STD_DEG", "ASPECT_SAMPLE_COUNT"])

        # Join tabulate area results (area per class)
        tab_case_field = [f.name for f in arcpy.ListFields(tab_area_tbl)][0]
        arcpy.management.JoinField(out_fc, zone_field, tab_area_tbl, tab_case_field, None)

        # Identify class fields that were added from TabulateArea
        tab_fields_tbl = [f for f in arcpy.ListFields(tab_area_tbl) if f.name != tab_case_field and f.type in ("Double", "Single", "Integer", "SmallInteger", "OID")]
        # Build mapping original field -> (area_field_name, pct_field_name)
        class_field_mappings = []
        for f in tab_fields_tbl:
            fname = f.name
            # try to extract the numeric code from the field name
            m = re.search(r'(\d+)', fname)
            code = None
            if m:
                code = int(m.group(1))
            else:
                # fallback: try to match by count/order if no digits found (use index)
                # find index in ListFields order
                idx = [ff.name for ff in arcpy.ListFields(tab_area_tbl)].index(fname)
                # try to map index-1 to class code if possible
                code = None

            label = class_definitions.get(code, str(code))
            area_field_name = f"SLOPE_CLASS_AREA_{label}_SQM"
            pct_field_name = f"SLOPE_PCT_{label}"
            # truncate names to 64 chars if necessary
            if len(area_field_name) > 64:
                area_field_name = area_field_name[:64]
            if len(pct_field_name) > 64:
                pct_field_name = pct_field_name[:64]
            class_field_mappings.append((fname, area_field_name, pct_field_name))

        # Add polygon area field and compute it
        area_field = "POLY_AREA_SQM"
        try:
            self._calculate_polygon_area(out_fc, area_field, messages)
        except Exception as e:
            tb = traceback.format_exc()
            raise arcpy.ExecuteError(f"Failed to calculate polygon areas: {e}\n{tb}")

        # For each class field created by TabulateArea, create clearer area and percent fields
        for orig_field, area_field_name, pct_field_name in class_field_mappings:
            # Add area field if missing
            if area_field_name not in [f.name for f in arcpy.ListFields(out_fc)]:
                arcpy.AddField_management(out_fc, area_field_name, "DOUBLE")
            # Copy values from orig_field into the clearer area field
            arcpy.management.CalculateField(out_fc, area_field_name, expression=f"!{orig_field}!", expression_type="PYTHON3")
            # Add percent field and calculate percent
            if pct_field_name not in [f.name for f in arcpy.ListFields(out_fc)]:
                arcpy.AddField_management(out_fc, pct_field_name, "DOUBLE")
            arcpy.management.CalculateField(out_fc, pct_field_name,
                                            expression=f"(!{area_field_name}! / !{area_field}!) * 100 if (!{area_field}! > 0) else 0",
                                            expression_type="PYTHON3")

        messages.addMessage(f"Output feature class with slope statistics created: {out_fc}")

        # Optionally add output to current map
        if add_to_map and aprx:
            try:
                # Prefer the active map if available, otherwise pick the first map
                map_obj = None
                try:
                    map_obj = aprx.activeMap
                except Exception:
                    map_obj = None
                if not map_obj:
                    maps = aprx.listMaps()
                    map_obj = maps[0] if maps else None

                if map_obj:
                    messages.addMessage(f"Adding output feature class to map '{map_obj.name}'")
                    map_obj.addDataFromPath(out_fc)
                    messages.addMessage("Added output to map.")
                else:
                    messages.addMessage("Could not find an open map in the project to add the output to.")
            except Exception as e:
                messages.addWarningMessage(f"Failed to add output to the map: {e}")

        # Cleanup
        try:
            for t in (elev_pts, slope_pts, aspect_pts, elev_pts_z, slope_pts_z, aspect_pts_z,
                      elev_stats_tbl, slope_stats_tbl, aspect_stats_tbl, class_rast_temp, tab_area_tbl, tin_rast):
                if t and arcpy.Exists(t):
                    arcpy.Delete_management(t)
        except Exception:
            pass

        messages.addMessage("Slope analysis completed successfully.")
        return
