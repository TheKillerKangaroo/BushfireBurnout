# -*- coding: utf-8 -*-
import arcpy
import os
from datetime import datetime

FEATURE_SERVICE_URL = r"https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/Project_Study_Area/FeatureServer/0"
SVTM_URL = r"https://mapprod3.environment.nsw.gov.au/arcgis/rest/services/VIS/SVTM_NSW_Extant_PCT/MapServer/3"
TARGET_EPSG = 8058
FD_NAME = "BufferLayers"
FD_ALT_NAME = "BufferLayers_EPSG8058"

def _msg(msg):
    arcpy.AddMessage(f"[MontyGIS] {msg}")

def _warn(msg):
    arcpy.AddWarning(f"[MontyGIS – Warning] {msg}")

def _ensure_fds(workspace):
    _msg("Summoning the Holy Spatial Reference (EPSG 8058) for the feature dataset...")
    arcpy.env.workspace = workspace
    sr_target = arcpy.SpatialReference(TARGET_EPSG)

    def _fds_sr_ok(fds_path):
        try:
            desc = arcpy.Describe(fds_path)
            return desc.spatialReference.factoryCode == TARGET_EPSG
        except Exception as ex:
            _warn(f"Spat upon by Describe while inspecting {fds_path}: {ex}")
            return False

    fds_pref = os.path.join(workspace, FD_NAME)
    fds_alt = os.path.join(workspace, FD_ALT_NAME)

    if arcpy.Exists(fds_pref):
        if _fds_sr_ok(fds_pref):
            _msg(f"Found existing feature dataset {fds_pref} in the proper projection. Nobody expects EPSG 8058!")
            return fds_pref, sr_target
        else:
            _warn(f"Feature dataset {fds_pref} is of dubious projection. We shall forge {fds_alt} instead.")

    if arcpy.Exists(fds_alt):
        _msg(f"Reusing alternate feature dataset {fds_alt}. It got better.")
    else:
        _msg(f"Creating alternate feature dataset {fds_alt}. Bring out your data!")
        arcpy.management.CreateFeatureDataset(workspace, FD_ALT_NAME, sr_target)

    return fds_alt, sr_target

def _delete_name_globally(gdb_workspace, name):
    _msg(f"Scouring geodatabase {gdb_workspace} for anything named '{name}' (like a cartographic inquisition)...")
    prev = arcpy.env.workspace
    arcpy.env.workspace = gdb_workspace

    for fc in arcpy.ListFeatureClasses(name):
        _msg(f"  • Executing feature class {fc}. It's a fair cop.")
        arcpy.management.Delete(fc)
    for ras in arcpy.ListRasters(name):
        _msg(f"  • Banishing raster {ras} to the abyss.")
        arcpy.management.Delete(ras)

    for ds in arcpy.ListDatasets(feature_type='feature'):
        ds_path = os.path.join(gdb_workspace, ds)
        arcpy.env.workspace = ds_path
        for fc in arcpy.ListFeatureClasses(name):
            _msg(f"  • Executing feature class {fc} inside {ds}.")
            arcpy.management.Delete(fc)
        for ras in arcpy.ListRasters(name):
            _msg(f"  • Banishing raster {ras} from {ds}.")
            arcpy.management.Delete(ras)

    arcpy.env.workspace = prev
    _msg("Global purge complete. Bring out the next dataset!")

def _unique_rename(path, data_type="FeatureClass"):
    if not arcpy.Exists(path):
        return path
    stamp = datetime.now().strftime("%Y%m%d")
    base = os.path.basename(path)
    parent = os.path.dirname(path)
    candidate = f"{base}_{stamp}"
    candidate_path = os.path.join(parent, candidate)
    i = 1
    while arcpy.Exists(candidate_path):
        candidate = f"{base}_{stamp}_{i}"
        candidate_path = os.path.join(parent, candidate)
        i += 1
    _msg(f"Object '{base}' already exists; renaming it to '{candidate}' (and pretending this was the plan all along).")
    arcpy.management.Rename(path, candidate, data_type)
    return path

def _prepare_output(path, overwrite, data_type="FeatureClass", gdb_workspace=None):
    name = os.path.basename(path)
    if overwrite:
        _msg(f"Overwrite is enabled. Silencing previous '{name}' with extreme prejudice...")
        if gdb_workspace:
            _delete_name_globally(gdb_workspace, name)
        elif arcpy.Exists(path):
            arcpy.management.Delete(path)
        return path
    else:
        _msg(f"Overwrite is disabled. We shall delicately sidestep name clashes for '{name}'.")
        return _unique_rename(path, data_type)

def _tin_output_path(workspace, tin_name):
    if workspace.lower().endswith(".gdb"):
        base_folder = os.path.dirname(workspace)
        tin_folder = os.path.join(base_folder, "TINs")
    else:
        tin_folder = os.path.join(workspace, "TINs")

    if not os.path.exists(tin_folder):
        _msg(f"Constructing TIN lair at '{tin_folder}'. Mind the gap.")
        os.makedirs(tin_folder, exist_ok=True)

    return os.path.join(tin_folder, tin_name)

class Toolbox(object):
    def __init__(self):
        self.label = "Buffer Toolbox V9"
        self.alias = "BufferToolboxV9"
        self.tools = [SiteBufferToolV9]

class SiteBufferToolV9(object):
    def __init__(self):
        self.label = "Bushfire Preliminary Assessment"
        self.description = (
            "Buffers the project, clips contours and SVTM, builds a TIN, buffers the building, "
            "erases the building, creates a 1 m DSM, and DSM Above/Below polygons. "
            "All without mentioning the war."
        )
        self.canRunInBackground = True

    def getParameterInfo(self):
        params = []

        p_ws = arcpy.Parameter(
            displayName="Output Workspace (GDB)",
            name="workspace",
            datatype="DEWorkspace",
            parameterType="Required",
            direction="Input"
        )

        p_project = arcpy.Parameter(
            displayName="Project Number",
            name="project_number",
            datatype="GPString",
            parameterType="Required",
            direction="Input"
        )
        p_project.filter.type = "ValueList"

        p_buffer = arcpy.Parameter(
            displayName="Site Buffer Distance (meters)",
            name="buffer_distance",
            datatype="GPDouble",
            parameterType="Required",
            direction="Input"
        )
        p_buffer.value = 200.0

        p_contour = arcpy.Parameter(
            displayName="2m Contour Feature Class (or layer)",
            name="contours_fc",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )

        p_building = arcpy.Parameter(
            displayName="Building Outline Feature Class (or layer)",
            name="building_fc",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )

        p_build_buffer = arcpy.Parameter(
            displayName="Building Buffer Distance (meters)",
            name="building_buffer_distance",
            datatype="GPDouble",
            parameterType="Required",
            direction="Input"
        )
        p_build_buffer.value = 140.0

        p_split = arcpy.Parameter(
            displayName="Elevation Threshold (meters)",
            name="split_elevation",
            datatype="GPDouble",
            parameterType="Required",
            direction="Input"
        )
        p_split.value = 0.0

        p_overwrite = arcpy.Parameter(
            displayName="Overwrite existing outputs",
            name="overwrite_outputs",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input"
        )
        p_overwrite.value = False

        p_addmap = arcpy.Parameter(
            displayName="Add outputs to current map",
            name="add_to_map",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input"
        )
        p_addmap.value = False

        params.extend([
            p_ws, p_project, p_buffer, p_contour,
            p_building, p_build_buffer, p_split,
            p_overwrite, p_addmap
        ])
        return params

    def updateParameters(self, parameters):
        if parameters[1].altered is False:
            try:
                _msg("Consulting the remote oracle for project numbers (feature service)...")
                with arcpy.da.SearchCursor(FEATURE_SERVICE_URL, ["project_number"]) as cursor:
                    vals = sorted({row[0] for row in cursor if row[0]})
                parameters[1].filter.list = vals
                _msg(f"Loaded {len(vals)} project numbers from on high.")
            except Exception as ex:
                _warn(f"Could not load project numbers from the feature service: {ex}")
        return

    def execute(self, parameters, messages):
        _msg("Welcome to the Bushfire Preliminary Assessment. Please do not adjust your contour lines.")

        workspace = parameters[0].valueAsText
        project_number = parameters[1].valueAsText
        buffer_distance = float(parameters[2].value)
        contours_fc = parameters[3].valueAsText
        building_fc = parameters[4].valueAsText
        building_buffer_distance = float(parameters[5].value)
        split_elev = float(parameters[6].value)
        overwrite_outputs = bool(parameters[7].value)
        add_to_map = bool(parameters[8].value)

        _msg(f"Workspace has been declared: {workspace}")
        _msg(f"Project of interest: {project_number}")
        _msg(f"Site buffer distance: {buffer_distance} m (a comfortable stroll).")
        _msg(f"Contours provided: {contours_fc}")
        _msg(f"Building outline: {building_fc}")
        _msg(f"Building buffer distance: {building_buffer_distance} m (for health and safety reasons).")
        _msg(f"Elevation threshold: {split_elev} m (above which things are 'tall' and below which they are 'not tall').")
        _msg(f"Overwrite outputs: {overwrite_outputs}")
        _msg(f"Add outputs to map: {add_to_map}")

        # Validate building geometry
        try:
            bdesc = arcpy.Describe(building_fc)
            if getattr(bdesc, "shapeType", "").lower() != "polygon":
                raise arcpy.ExecuteError(
                    f"Building feature class must be polygon. Found: {bdesc.shapeType}. "
                    f"This is an ex-building geometry."
                )
        except Exception as ex:
            raise arcpy.ExecuteError(f"Could not validate building feature class geometry: {ex}")

        fds_path, sr = _ensure_fds(workspace)

        # Subject site selection
        safe_project = project_number.replace("'", "''")
        where = f"project_number = '{safe_project}'"
        subject_layer = "subject_site_layer"
        _msg(f"Conjuring subject site from feature service with where-clause: {where}")
        arcpy.management.MakeFeatureLayer(FEATURE_SERVICE_URL, subject_layer, where)
        if int(arcpy.management.GetCount(subject_layer).getOutput(0)) == 0:
            raise arcpy.ExecuteError(
                f"No features found for project_number {project_number}. "
                f"Your project appears to have joined the choir invisible."
            )

        # Site buffer
        _msg("Applying site buffer. Stand well back; it may go off.")
        buffer_name = f"Site_Buffer_{int(buffer_distance)}"
        buffer_path = os.path.join(fds_path, buffer_name)
        buffer_path = _prepare_output(buffer_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Buffer(subject_layer, buffer_path, f"{buffer_distance} Meters", dissolve_option="ALL")
        _msg(f"Site buffer created at {buffer_path}.")

        # Contours clip
        _msg("Clipping contours to the site buffer. No contour shall pass (the boundary).")
        clipped_name = f"AEP{project_number}2m_Contours"
        clipped_path = os.path.join(fds_path, clipped_name)
        clipped_path = _prepare_output(clipped_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Clip(contours_fc, buffer_path, clipped_path)
        _msg(f"Contours clipped into {clipped_path}.")

        # SVTM clip to site buffer
        svtm_date = datetime.now().strftime("%Y%m%d")
        svtm_name = f"AEP{project_number}_SVTM_{svtm_date}"
        svtm_path = os.path.join(fds_path, svtm_name)
        svtm_path = _prepare_output(svtm_path, overwrite_outputs, "FeatureClass", workspace)
        _msg("Summoning SVTM layer from distant lands and clipping to site buffer...")
        arcpy.management.MakeFeatureLayer(SVTM_URL, "svtm_layer")
        arcpy.analysis.Clip("svtm_layer", buffer_path, svtm_path)
        _msg(f"SVTM clipped to {svtm_path}.")

        # TIN from clipped contours
        tin_name = f"AEP{project_number}_TIN"
        tin_path = _tin_output_path(workspace, tin_name)
        if arcpy.Exists(tin_path):
            _msg(f"A previous TIN was found at {tin_path}; it has been sacked.")
            arcpy.management.Delete(tin_path)

        z_field = self._infer_z_field(clipped_path)
        _msg(f"Using elevation field '{z_field}' for TIN creation. It's only a model.")
        in_feats = [[clipped_path, z_field, "hardline"]]
        _msg(f"TIN inputs: {in_feats}")
        _msg(f"Creating TIN at {tin_path}...")
        arcpy.ddd.CreateTin(out_tin=tin_path, spatial_reference=sr,
                            in_features=in_feats, constrained_delaunay="DELAUNAY")
        _msg("TIN successfully created. You may now pretend to be King Arthur of the Triangulated Realm.")

        # Building buffer
        _msg("Buffering building outline. Because one must respect personal space.")
        bbuf_name = f"ARP{project_number}_Building_Buffer_{int(building_buffer_distance)}M"
        bbuf_path = os.path.join(fds_path, bbuf_name)
        bbuf_path = _prepare_output(bbuf_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Buffer(building_fc, bbuf_path, f"{building_buffer_distance} Meters", dissolve_option="ALL")
        _msg(f"Building buffer created at {bbuf_path}.")

        # SVTM clip to building buffer
        _msg("Clipping SVTM to the building buffer, like trimming a very ecological hedge.")
        svtm_bbuf_name = f"AEP{project_number}_SVTM_Bld_Buffer_{svtm_date}"
        svtm_bbuf_path = os.path.join(fds_path, svtm_bbuf_name)
        svtm_bbuf_path = _prepare_output(svtm_bbuf_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Clip(svtm_path, bbuf_path, svtm_bbuf_path)
        _msg(f"SVTM within building buffer saved to {svtm_bbuf_path}.")

        # Erase buildings from SVTM building buffer
        _msg("Erasing the building footprint from the SVTM building buffer. 'Now you see it, now you don't.'")
        svtm_bbuf_erase_name = f"AEP{project_number}_SVTM_Bld_Buffer_NoBld_{svtm_date}"
        svtm_bbuf_erase_path = os.path.join(fds_path, svtm_bbuf_erase_name)
        svtm_bbuf_erase_path = _prepare_output(svtm_bbuf_erase_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Erase(svtm_bbuf_path, building_fc, svtm_bbuf_erase_path)
        _msg(f"Building erased from SVTM buffer; results at {svtm_bbuf_erase_path}.")

        # DSM from TIN
        _msg("Transmuting TIN into a 1 m DSM (masked to site buffer). This may tingle slightly.")
        old_cell = arcpy.env.cellSize
        arcpy.env.mask = buffer_path
        arcpy.env.extent = buffer_path
        arcpy.env.cellSize = 1

        dsm_tmp = os.path.join("in_memory", "dsm_1m")
        arcpy.ddd.TinRaster(tin_path, dsm_tmp, "FLOAT", "LINEAR", "CELLSIZE", 1)
        dsm_name = f"AEP{project_number}_DSM_1m"
        dsm_path = os.path.join(workspace, dsm_name)
        dsm_path = _prepare_output(dsm_path, overwrite_outputs, "RasterDataset", workspace)
        arcpy.management.CopyRaster(dsm_tmp, dsm_path, pixel_type="32_BIT_FLOAT")
        _msg(f"DSM safely tucked into {dsm_path}.")

        # Polygonize DSM
        _msg("Integerizing DSM and conjuring polygons. Beware of jagged edges.")
        dsm_int = arcpy.sa.Int(arcpy.sa.Raster(dsm_path))
        dsm_poly_name = f"AEP{project_number}_DSM_1m_Polys"
        dsm_poly_path = os.path.join(fds_path, dsm_poly_name)
        dsm_poly_path = _prepare_output(dsm_poly_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.conversion.RasterToPolygon(dsm_int, dsm_poly_path, "SIMPLIFY")
        _msg(f"DSM polygons created at {dsm_poly_path}.")

        if arcpy.Exists(dsm_poly_path):
            _msg("Adding 'Elevation' field and stuffing it with gridcode values (now in double flavour).")
            if "Elevation" not in [f.name for f in arcpy.ListFields(dsm_poly_path)]:
                arcpy.AddField_management(dsm_poly_path, "Elevation", "DOUBLE")
            with arcpy.da.UpdateCursor(dsm_poly_path, ["gridcode", "Elevation"]) as cur:
                for row in cur:
                    row[1] = float(row[0])
                    cur.updateRow(row)

        # Above/below threshold
        _msg(f"Separating terrain into 'Higher than {split_elev}' and 'Not Quite So High'.")
        greater_tmp = os.path.join("in_memory", "dsm_greater")
        lesseq_tmp = os.path.join("in_memory", "dsm_lesseq")

        arcpy.analysis.Select(dsm_poly_path, greater_tmp, f"Elevation > {split_elev}")
        arcpy.analysis.Select(dsm_poly_path, lesseq_tmp, f"Elevation <= {split_elev}")

        greater_diss = os.path.join("in_memory", "dsm_greater_diss")
        lesseq_diss = os.path.join("in_memory", "dsm_lesseq_diss")
        if int(arcpy.management.GetCount(greater_tmp).getOutput(0)) > 0:
            _msg("Dissolving all 'Higher' polygons into a single mighty blob.")
            arcpy.management.Dissolve(greater_tmp, greater_diss)
        else:
            _warn("No polygons found above the threshold. Everything is terribly low.")

        if int(arcpy.management.GetCount(lesseq_tmp).getOutput(0)) > 0:
            _msg("Dissolving all 'Not Quite So High' polygons into a single less‑impressive blob.")
            arcpy.management.Dissolve(lesseq_tmp, lesseq_diss)
        else:
            _warn("No polygons found below or equal to the threshold. Everything is terribly high.")

        final_name = f"AEP{project_number}_DSM_AboveBelow_{svtm_date}"
        final_path = os.path.join(fds_path, final_name)
        final_path = _prepare_output(final_path, overwrite_outputs, "FeatureClass", workspace)
        parts = []
        if arcpy.Exists(greater_diss):
            arcpy.AddField_management(greater_diss, "Relation", "TEXT", field_length=10)
            with arcpy.da.UpdateCursor(greater_diss, ["Relation"]) as cur:
                for row in cur:
                    row[0] = "Greater"
                    cur.updateRow(row)
            parts.append(greater_diss)

        if arcpy.Exists(lesseq_diss):
            arcpy.AddField_management(lesseq_diss, "Relation", "TEXT", field_length=10)
            with arcpy.da.UpdateCursor(lesseq_diss, ["Relation"]) as cur:
                for row in cur:
                    row[0] = "LessEqual"
                    cur.updateRow(row)
            parts.append(lesseq_diss)

        if parts:
            _msg("Merging 'Greater' and 'LessEqual' into a single Above/Below layer. It's only a model.")
            arcpy.management.Merge(parts, final_path)
            _msg(f"DSM Above/Below polygons created at {final_path}.")
        else:
            _warn("No Above or Below polygons to merge. Possibly flat as a pancake.")

        # Reset env, cleanup
        arcpy.env.cellSize = old_cell
        arcpy.env.mask = None
        arcpy.env.extent = None
        arcpy.env.outputCoordinateSystem = None
        arcpy.management.Delete("in_memory")

        # Add outputs to map
        if add_to_map:
            _msg("Adding key outputs to the current map. Cue triumphant fanfare.")
            self._add_outputs_to_map([
                buffer_path, clipped_path, svtm_path, tin_path,
                bbuf_path, svtm_bbuf_path, svtm_bbuf_erase_path,
                dsm_path, dsm_poly_path, final_path
            ])
        else:
            _msg("Not adding outputs to the map by request. They are lurking in the geodatabase, sniggering quietly.")

        _msg("Bushfire Preliminary Assessment completed. And now for something completely different: manual QA.")
        return

    def _infer_z_field(self, fc):
        _msg(f"Attempting to divine the elevation field in {fc}...")
        fields = [f for f in arcpy.ListFields(fc)
                  if f.type in ("Integer", "SmallInteger", "Double", "Single")]
        candidates = ("ELEVATION", "ELEV", "Z", "CONTOUR", "VALUE")
        for cand in candidates:
            for f in fields:
                if f.name.upper() == cand:
                    _msg(f"Found promising elevation field '{f.name}'. That'll do nicely.")
                    return f.name
        if fields:
            _warn(f"No standard elevation field found; defaulting to first numeric field '{fields[0].name}'. Spam, spam, spam, ELEVATION.")
            return fields[0].name
        raise arcpy.ExecuteError("No numeric elevation field found for contours. This DEM is deceased.")

    def _add_outputs_to_map(self, paths):
        try:
            aprx = arcpy.mp.ArcGISProject("CURRENT")
            m = aprx.activeMap
            if not m:
                _warn("No active map detected. Your layers are free, like swallows (possibly African).")
                return
            for lyr_path in paths:
                try:
                    if not lyr_path or not arcpy.Exists(lyr_path):
                        continue
                    m.addDataFromPath(lyr_path)
                    _msg(f"Layer added to map: {lyr_path}")
                except Exception as ex_inner:
                    _warn(f"Could not add {lyr_path} to map; it has shuffled off this visible coil: {ex_inner}")
            _msg("All feasible layers have been hoisted into the map.")
        except Exception as ex:
            _warn(f"Could not add outputs to map: {ex}. Perhaps the map has gone to lunch.")
