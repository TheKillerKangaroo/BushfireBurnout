# -*- coding: utf-8 -*-
import arcpy
import os
import math
import uuid
import tempfile
import traceback
import re
from datetime import datetime

FEATURE_SERVICE_URL = r"https://services-ap1.arcgis.com/1awYJ9qmpKeoPyqc/arcgis/rest/services/Project_Study_Area/FeatureServer/0"
SVTM_URL = r"https://mapprod3.environment.nsw.gov.au/arcgis/rest/services/VIS/SVTM_NSW_Extant_PCT/MapServer/3"
BFPL_URL = r"https://mapprod3.environment.nsw.gov.au/arcgis/rest/services/Fire/BFPL/MapServer/0"
TARGET_EPSG = 8058
FD_NAME = "BufferLayers"
FD_ALT_NAME = "BufferLayers_EPSG8058"

def _msg(msg):
    arcpy.AddMessage(msg)

def _warn(msg):
    arcpy.AddWarning(msg)

def _ensure_fds(workspace):
    _msg("Ensuring feature dataset exists in EPSG:8058...")
    arcpy.env.workspace = workspace
    sr_target = arcpy.SpatialReference(TARGET_EPSG)

    def _fds_sr_ok(fds_path):
        try:
            desc = arcpy.Describe(fds_path)
            return desc.spatialReference.factoryCode == TARGET_EPSG
        except Exception as ex:
            _warn(f"Describe failed for '{fds_path}': {ex}")
            return False

    fds_pref = os.path.join(workspace, FD_NAME)
    fds_alt = os.path.join(workspace, FD_ALT_NAME)

    if arcpy.Exists(fds_pref):
        if _fds_sr_ok(fds_pref):
            _msg(f"Using feature dataset: {fds_pref} (EPSG:8058).")
            return fds_pref, sr_target
        else:
            _warn(f"Feature dataset '{fds_pref}' is not EPSG:8058. Using '{fds_alt}'.")

    if arcpy.Exists(fds_alt):
        _msg(f"Using alternate feature dataset: {fds_alt}.")
    else:
        _msg(f"Creating feature dataset: {fds_alt} (EPSG:8058).")
        arcpy.management.CreateFeatureDataset(workspace, FD_ALT_NAME, sr_target)

    return fds_alt, sr_target

def _delete_name_globally(gdb_workspace, name):
    _msg(f"Deleting existing items named '{name}' across geodatabase...")
    prev = arcpy.env.workspace
    try:
        arcpy.env.workspace = gdb_workspace

        root_candidate = os.path.join(gdb_workspace, name)
        try:
            if arcpy.Exists(root_candidate):
                arcpy.management.Delete(root_candidate)
                _msg(f"Deleted root item: {root_candidate}")
        except Exception as ex_root:
            _warn(f"Delete failed for root '{root_candidate}': {ex_root}")

        try:
            for fc in arcpy.ListFeatureClasses(name):
                try:
                    arcpy.management.Delete(fc)
                    _msg(f"Deleted feature class: {fc}")
                except Exception as ex_fc:
                    _warn(f"Delete failed for feature class '{fc}': {ex_fc}")
        except Exception as ex_list_root:
            _warn(f"ListFeatureClasses failed in root: {ex_list_root}")

        try:
            for ras in arcpy.ListRasters(name):
                try:
                    arcpy.management.Delete(ras)
                    _msg(f"Deleted raster: {ras}")
                except Exception as ex_r:
                    _warn(f"Delete failed for raster '{ras}': {ex_r}")
        except Exception as ex_list_rast:
            _warn(f"ListRasters failed in root: {ex_list_rast}")

        try:
            for ds in arcpy.ListDatasets(feature_type='feature') or []:
                ds_path = os.path.join(gdb_workspace, ds)
                candidate_in_ds = os.path.join(ds_path, name)
                try:
                    if arcpy.Exists(candidate_in_ds):
                        arcpy.management.Delete(candidate_in_ds)
                        _msg(f"Deleted in dataset '{ds}': {candidate_in_ds}")
                except Exception as ex_cds:
                    _warn(f"Delete failed in dataset '{ds}' for '{candidate_in_ds}': {ex_cds}")

                try:
                    arcpy.env.workspace = ds_path
                    for fc in arcpy.ListFeatureClasses(name):
                        try:
                            arcpy.management.Delete(fc)
                            _msg(f"Deleted feature class in dataset '{ds}': {fc}")
                        except Exception as ex_fc_ds:
                            _warn(f"Delete failed in dataset '{ds}' for feature class '{fc}': {ex_fc_ds}")
                    for ras in arcpy.ListRasters(name):
                        try:
                            arcpy.management.Delete(ras)
                            _msg(f"Deleted raster in dataset '{ds}': {ras}")
                        except Exception as ex_rds:
                            _warn(f"Delete failed in dataset '{ds}' for raster '{ras}': {ex_rds}")
                except Exception as ex_inner_ds:
                    _warn(f"Enumeration failed inside dataset '{ds}': {ex_inner_ds}")
        except Exception as ex_ds_list:
            _warn(f"ListDatasets failed: {ex_ds_list}")

    finally:
        arcpy.env.workspace = prev
    _msg("Global delete complete.")

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
    _msg(f"Existing name detected; renaming '{base}' to '{candidate}'.")
    arcpy.management.Rename(path, candidate, data_type)
    return candidate_path

def _prepare_output(path, overwrite, data_type="FeatureClass", gdb_workspace=None, geometry_type=None, spatial_ref=None):
    name = os.path.basename(path)
    if overwrite and arcpy.Exists(path):
        _msg(f"Overwrite enabled; removing existing '{name}'.")
        arcpy.management.Delete(path)
    elif arcpy.Exists(path):
        _msg(f"Overwrite disabled; ensuring unique name for '{name}'.")
        return _unique_rename(path, data_type)

    if not arcpy.Exists(path) and geometry_type:
        parent_dir = os.path.dirname(path)
        base_name = os.path.basename(path)
        _msg(f"Executing CreateFeatureclass for '{base_name}' with {geometry_type} geometry.")
        arcpy.management.CreateFeatureclass(parent_dir, base_name, geometry_type, spatial_reference=spatial_ref)
    
    return path

def _tin_output_path(workspace, tin_name):
    if workspace.lower().endswith(".gdb"):
        base_folder = os.path.dirname(workspace)
        tin_folder = os.path.join(base_folder, "TINs")
    else:
        tin_folder = os.path.join(workspace, "TINs")
    if not os.path.exists(tin_folder):
        _msg(f"Creating TIN output folder: {tin_folder}")
        os.makedirs(tin_folder, exist_ok=True)
    return os.path.join(tin_folder, tin_name)

class Toolbox(object):
    def __init__(self):
        self.label = "Buffer Toolbox V10"
        self.alias = "BufferToolboxV10"
        self.tools = [BushfireToolboxV10]

class BushfireToolboxV10(object):
    def __init__(self):
        self.label = "Bushfire Preliminary Assessment (V10)"
        self.description = (
            "Generates buffers, clips contours/SVTM/BFPL, builds a TIN and DSM, classifies elevation (above/below threshold), overlays onto SVTM, and runs slope analysis. Calculates APZ and effectives slopes against SVTM Building Buffer (no buildings) polygons."
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
            displayName="Elevation Threshold (meters) for Above/Below",
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
        p_addmap.value = True

        params.extend([
            p_ws, p_project, p_buffer, p_contour,
            p_building, p_build_buffer, p_split,
            p_overwrite, p_addmap
        ])
        return params

    def updateParameters(self, parameters):
        if parameters[1].altered is False:
            try:
                _msg("Loading project numbers from feature service...")
                with arcpy.da.SearchCursor(FEATURE_SERVICE_URL, ["project_number"]) as cursor:
                    vals = sorted({row[0] for row in cursor if row[0]})
                parameters[1].filter.list = vals
                _msg(f"Project numbers loaded: {len(vals)}")
            except Exception as ex:
                _warn(f"Project number load failed: {ex}")
        return

    def execute(self, parameters, messages):
        _msg("Starting Bushfire Preliminary Assessment (V10).")

        workspace = parameters[0].valueAsText
        project_number = parameters[1].valueAsText
        buffer_distance = float(parameters[2].value)
        contours_fc = parameters[3].valueAsText
        building_fc = parameters[4].valueAsText
        building_buffer_distance = float(parameters[5].value)
        split_elev = float(parameters[6].value)
        overwrite_outputs = bool(parameters[7].value)
        add_to_map = bool(parameters[8].value)

        _msg(f"Workspace: {workspace}")
        _msg(f"Project: {project_number}")
        _msg(f"Site buffer: {buffer_distance} m")
        _msg(f"Contours: {contours_fc}")
        _msg(f"Building outlines: {building_fc}")
        _msg(f"Building buffer: {building_buffer_distance} m")
        _msg(f"Threshold (Above/Below): {split_elev} m")
        _msg(f"Overwrite outputs: {overwrite_outputs}")
        _msg(f"Add to map: {add_to_map}")

        # Validate building geometry
        try:
            bdesc = arcpy.Describe(building_fc)
            if getattr(bdesc, "shapeType", "").lower() != "polygon":
                raise arcpy.ExecuteError(f"Building feature class must be polygon; found '{bdesc.shapeType}'.")
        except Exception as ex:
            raise arcpy.ExecuteError(f"Building feature class validation failed: {ex}")

        fds_path, sr = _ensure_fds(workspace)

        # Subject site selection
        safe_project = project_number.replace("'", "''")
        where = f"project_number = '{safe_project}'"
        subject_layer = "subject_site_layer"
        _msg(f"Selecting subject site with WHERE: {where}")
        arcpy.management.MakeFeatureLayer(FEATURE_SERVICE_URL, subject_layer, where)
        if int(arcpy.management.GetCount(subject_layer).getOutput(0)) == 0:
            raise arcpy.ExecuteError(f"No features found for project_number '{project_number}'.")

        # Site buffer
        buffer_name = f"AEP{project_number}_Site_Buffer_{int(buffer_distance)}"
        buffer_path = os.path.join(fds_path, buffer_name)
        buffer_path = _prepare_output(buffer_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Buffer(subject_layer, buffer_path, f"{buffer_distance} Meters", dissolve_option="ALL")
        _msg(f"Site buffer created: {buffer_path}")

        # Contours clip
        clipped_name = f"AEP{project_number}_2m_Contours"
        clipped_path = os.path.join(fds_path, clipped_name)
        clipped_path = _prepare_output(clipped_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Clip(contours_fc, buffer_path, clipped_path)
        _msg(f"Contours clipped: {clipped_path}")

        # SVTM clip to site buffer
        svtm_date = datetime.now().strftime("%Y%m%d")
        svtm_name = f"AEP{project_number}_SVTM_{svtm_date}"
        svtm_path = os.path.join(fds_path, svtm_name)
        svtm_path = _prepare_output(svtm_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.management.MakeFeatureLayer(SVTM_URL, "svtm_layer")
        arcpy.analysis.Clip("svtm_layer", buffer_path, svtm_path)
        _msg(f"SVTM clipped: {svtm_path}")

        # BFPL clip to site buffer
        bfpl_path = None
        try:
            bfpl_name = f"AEP{project_number}_BFPL_{svtm_date}"
            bfpl_path = os.path.join(fds_path, bfpl_name)
            bfpl_path = _prepare_output(bfpl_path, overwrite_outputs, "FeatureClass", workspace)
            arcpy.management.MakeFeatureLayer(BFPL_URL, "bfpl_layer")
            arcpy.analysis.Clip("bfpl_layer", buffer_path, bfpl_path)
            _msg(f"BFPL clipped: {bfpl_path}")
        except Exception as ex_bfpl:
            _warn(f"BFPL clip failed: {ex_bfpl}")

        # Building buffer
        bbuf_name = f"AEP{project_number}_Building_Buffer_{int(building_buffer_distance)}M"
        bbuf_path = os.path.join(fds_path, bbuf_name)
        bbuf_path = _prepare_output(bbuf_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Buffer(building_fc, bbuf_path, f"{building_buffer_distance} Meters", dissolve_option="ALL")
        _msg(f"Building buffer created: {bbuf_path}")

        # SVTM clip to building buffer
        svtm_bbuf_name = f"AEP{project_number}_SVTM_Bld_Buffer_{svtm_date}"
        svtm_bbuf_path = os.path.join(fds_path, svtm_bbuf_name)
        svtm_bbuf_path = _prepare_output(svtm_bbuf_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Clip(svtm_path, bbuf_path, svtm_bbuf_path)
        _msg(f"SVTM clipped to building buffer: {svtm_bbuf_path}")

        # Erase buildings from SVTM building buffer
        svtm_bbuf_erase_name = f"AEP{project_number}_SVTM_Bld_Buffer_NoBld_{svtm_date}"
        svtm_bbuf_erase_path = os.path.join(fds_path, svtm_bbuf_erase_name)
        svtm_bbuf_erase_path = _prepare_output(svtm_bbuf_erase_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Erase(svtm_bbuf_path, building_fc, svtm_bbuf_erase_path)
        _msg(f"SVTM building buffer (buildings erased): {svtm_bbuf_erase_path}")

        # TIN from clipped contours
        tin_name = f"AEP{project_number}_TIN"
        tin_path = _tin_output_path(workspace, tin_name)
        if arcpy.Exists(tin_path):
            _msg(f"Removing existing TIN: {tin_path}")
            arcpy.management.Delete(tin_path)
        z_field = self._infer_z_field(clipped_path)
        _msg(f"TIN elevation field: {z_field}")
        in_feats = [[clipped_path, z_field, "hardline"]]
        _msg(f"Creating TIN: {tin_path}")
        arcpy.ddd.CreateTin(out_tin=tin_path, spatial_reference=sr, in_features=in_feats, constrained_delaunay="DELAUNAY")
        _msg("TIN created")

        # DSM from TIN, Above/Below splitting with threshold
        _msg("Creating DSM (1 m) from TIN and classifying polygons by threshold...")
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
        _msg(f"DSM saved: {dsm_path}")

        _msg("Converting DSM to polygons and populating elevation...")
        dsm_int = arcpy.sa.Int(arcpy.sa.Raster(dsm_path))
        dsm_poly_name = f"AEP{project_number}_DSM_1m_Polys"
        dsm_poly_path = os.path.join(fds_path, dsm_poly_name)
        dsm_poly_path = _prepare_output(dsm_poly_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.conversion.RasterToPolygon(dsm_int, dsm_poly_path, "SIMPLIFY")
        if arcpy.Exists(dsm_poly_path):
            if "Elevation" not in [f.name for f in arcpy.ListFields(dsm_poly_path)]:
                arcpy.AddField_management(dsm_poly_path, "Elevation", "DOUBLE")
            with arcpy.da.UpdateCursor(dsm_poly_path, ["gridcode", "Elevation"]) as cur:
                for row in cur:
                    row[1] = float(row[0])
                    cur.updateRow(row)

        _msg(f"Selecting and dissolving polygons above/below {split_elev} m...")
        greater_tmp = os.path.join("in_memory", "dsm_greater")
        lesseq_tmp = os.path.join("in_memory", "dsm_lesseq")
        arcpy.analysis.Select(dsm_poly_path, greater_tmp, f"Elevation > {split_elev}")
        arcpy.analysis.Select(dsm_poly_path, lesseq_tmp, f"Elevation <= {split_elev}")

        greater_diss = os.path.join("in_memory", "dsm_greater_diss")
        lesseq_diss = os.path.join("in_memory", "dsm_lesseq_diss")
        parts = []
        if int(arcpy.management.GetCount(greater_tmp).getOutput(0)) > 0:
            arcpy.management.Dissolve(greater_tmp, greater_diss)
            arcpy.AddField_management(greater_diss, "Relation", "TEXT", field_length=10)
            with arcpy.da.UpdateCursor(greater_diss, ["Relation"]) as cur:
                for row in cur:
                    row[0] = "Greater"
                    cur.updateRow(row)
            parts.append(greater_diss)
        else:
            _warn("No polygons above threshold.")

        if int(arcpy.management.GetCount(lesseq_tmp).getOutput(0)) > 0:
            arcpy.management.Dissolve(lesseq_tmp, lesseq_diss)
            arcpy.AddField_management(lesseq_diss, "Relation", "TEXT", field_length=10)
            with arcpy.da.UpdateCursor(lesseq_diss, ["Relation"]) as cur:
                for row in cur:
                    row[0] = "LessEqual"
                    cur.updateRow(row)
            parts.append(lesseq_diss)
        else:
            _warn("No polygons below/equal to threshold.")

        # Requested name format: AEP{project_number}_Slope_Classification_{threshold}m_{date}
        final_name = f"AEP{project_number}_Slope_Classification_{int(split_elev)}m_{svtm_date}"
        final_path = os.path.join(fds_path, final_name)
        final_path = _prepare_output(final_path, overwrite_outputs, "FeatureClass", workspace, geometry_type="POLYGON", spatial_ref=sr)
        if parts:
            arcpy.management.Merge(parts, final_path)
            _msg(f"Slope classification polygons created: {final_path}")

            # Map Relation to Slope_Type on classification layer
            fields_final = [f.name for f in arcpy.ListFields(final_path)]
            if "Slope_Type" not in fields_final:
                arcpy.AddField_management(final_path, "Slope_Type", "TEXT", field_length=30)
            with arcpy.da.UpdateCursor(final_path, ["Relation", "Slope_Type"]) as ucur_final:
                for urow in ucur_final:
                    rel = (urow[0] or "").strip()
                    if rel == "LessEqual":
                        mapped = "Up Slope"
                    elif rel == "Greater":
                        mapped = "Down Slope"
                    else:
                        mapped = rel if rel else None
                    urow[1] = mapped
                    ucur_final.updateRow(urow)
            try:
                arcpy.management.DeleteField(final_path, "Relation")
            except Exception as ex_del_rel_final:
                _warn(f"Delete 'Relation' on classification layer failed: {ex_del_rel_final}")
        else:
            _warn("No classification polygons to merge.")

        # Overlay Above/Below onto SVTM layers via Identity, map Relation -> Slope_Type
        overlay_ok = True
        try:
            if not final_path or not arcpy.Exists(final_path):
                _warn("Classification layer missing; skipping SVTM overlay.")
                overlay_ok = False
            else:
                svtm_variants = [
                    ("SVTM (site)", svtm_path),
                    ("SVTM Building Buffer No Building", svtm_bbuf_erase_path)
                ]
                for label, svtm_fc in svtm_variants:
                    if not svtm_fc or not arcpy.Exists(svtm_fc):
                        _warn(f"SVTM source missing; skipping '{label}': {svtm_fc}")
                        overlay_ok = False
                        continue

                    _msg(f"Applying Identity overlay: {label}")
                    temp_svtm = os.path.join("in_memory", f"svtm_temp_{datetime.now().strftime('%H%M%S')}")
                    ident_tmp = os.path.join("in_memory", f"svtm_ident_{datetime.now().strftime('%H%M%S')}")

                    arcpy.management.CopyFeatures(svtm_fc, temp_svtm)

                    existing_fields = [f.name for f in arcpy.ListFields(temp_svtm)]
                    if "Relation" in existing_fields:
                        try:
                            arcpy.management.DeleteField(temp_svtm, "Relation")
                        except Exception as ex_del:
                            _warn(f"Delete existing 'Relation' failed for {label}: {ex_del}")

                    arcpy.analysis.Identity(temp_svtm, final_path, ident_tmp)

                    try:
                        if arcpy.Exists(svtm_fc):
                            arcpy.management.Delete(svtm_fc)
                        arcpy.management.CopyFeatures(ident_tmp, svtm_fc)

                        fields_after = [f.name for f in arcpy.ListFields(svtm_fc)]
                        if "Slope_Type" not in fields_after and "Relation" in fields_after:
                            arcpy.AddField_management(svtm_fc, "Slope_Type", "TEXT", field_length=30)
                            with arcpy.da.UpdateCursor(svtm_fc, ["Relation", "Slope_Type"]) as ucur:
                                for urow in ucur:
                                    rel = (urow[0] or "").strip()
                                    if rel == "LessEqual":
                                        mapped = "Up Slope"
                                    elif rel == "Greater":
                                        mapped = "Down Slope"
                                    else:
                                        mapped = rel if rel else None
                                    urow[1] = mapped
                                    ucur.updateRow(urow)
                            try:
                                arcpy.management.DeleteField(svtm_fc, "Relation")
                            except Exception as ex_del_rel:
                                _warn(f"Delete 'Relation' failed for {label}: {ex_del_rel}")
                        _msg(f"Overlay complete: {label}")
                    except Exception as ex_copy:
                        overlay_ok = False
                        _warn(f"Overlay copy failed for {label}: {ex_copy}")
        except Exception as ex_ident:
            overlay_ok = False
            _warn(f"SVTM overlay failed: {ex_ident}")

        # Restore env
        arcpy.env.cellSize = old_cell
        arcpy.env.mask = None
        arcpy.env.extent = None
        arcpy.env.outputCoordinateSystem = None

        # Run slope analysis and CAPTURE OUTPUT FC paths
        svtm_site_slope_fc = None
        svtm_bbuf_slope_fc = None
        if overlay_ok and final_path and arcpy.Exists(final_path):
            try:
                svtm_site_slope_fc = self._run_slope_analysis(in_tin=tin_path, in_polygons=svtm_path, add_to_map=False, label="SVTM (site)")
            except Exception as ex:
                _warn(f"Slope analysis failed (SVTM site): {ex}")
            try:
                svtm_bbuf_slope_fc = self._run_slope_analysis(in_tin=tin_path, in_polygons=svtm_bbuf_erase_path, add_to_map=False, label="SVTM Building Buffer No Building")
            except Exception as ex:
                _warn(f"Slope analysis failed (SVTM bld buffer no bld): {ex}")
        else:
            _warn("Skipping slope analysis: overlay incomplete or classification layer missing.")

        # APZ Assessment and Visualization
        apz_assessment_fc = None
        apz_lines_fc = None
        apz_poly_fc = None
        try:
            target_fc = svtm_bbuf_slope_fc
            if not (target_fc and arcpy.Exists(target_fc)):
                _warn("Slope analysis output missing; skipping APZ Assessment.")
            else:
                _msg("--- Initiating APZ Assessment Protocol ---")
                
                # Create a copy for the final APZ Assessment layer
                apz_assessment_name = f"AEP{project_number}_APZ_Assessment_{svtm_date}"
                apz_assessment_fc = os.path.join(fds_path, apz_assessment_name)
                apz_assessment_fc = _prepare_output(apz_assessment_fc, overwrite_outputs, "FeatureClass", workspace, geometry_type="POLYGON", spatial_ref=sr)
                arcpy.management.CopyFeatures(target_fc, apz_assessment_fc)
                _msg(f"Assessment layer instantiated: {apz_assessment_fc}")

                # Ensure fields exist
                field_names = [f.name for f in arcpy.ListFields(apz_assessment_fc)]
                if "Keith_Match" not in field_names: arcpy.AddField_management(apz_assessment_fc, "Keith_Match", "TEXT", field_length=120)
                if "Effective_Slope" not in field_names: arcpy.AddField_management(apz_assessment_fc, "Effective_Slope", "TEXT", field_length=20)
                if "APZ_Distance_M" not in field_names: arcpy.AddField_management(apz_assessment_fc, "APZ_Distance_M", "LONG")

                vegclass_field = next((cand for cand in ("vegClass", "VegClass", "VEGCLASS") if cand in field_names), None)
                if not vegclass_field: raise arcpy.ExecuteError("vegClass field not found.")

                apz_table = {
                    "Rainforest": {"Up slopes and flat": 38, ">0-5°": 47, ">5-10°": 57, ">10-15°": 69, ">15-20°": 81},
                    "Forest (wet and dry sclerophyll) including Coastal Swamp Forest, Pine Plantations and Sub-Alpine Woodland": {"Up slopes and flat": 67, ">0-5°": 79, ">5-10°": 93, ">10-15°": 100, ">15-20°": 100},
                    "Grassy and Semi-Arid Woodland (including Mallee)": {"Up slopes and flat": 42, ">0-5°": 50, ">5-10°": 60, ">10-15°": 72, ">15-20°": 85},
                    "Forested Wetland (excluding Coastal Swamp Forest)": {"Up slopes and flat": 34, ">0-5°": 42, ">5-10°": 51, ">10-15°": 62, ">15-20°": 73},
                    "Tall Heath": {"Up slopes and flat": 50, ">0-5°": 56, ">5-10°": 61, ">10-15°": 67, ">15-20°": 72},
                    "Short Heath": {"Up slopes and flat": 33, ">0-5°": 37, ">5-10°": 41, ">10-15°": 45, ">15-20°": 49},
                    "Arid-Shrublands (acacia and chenopod)": {"Up slopes and flat": 24, ">0-5°": 27, ">5-10°": 30, ">10-15°": 34, ">15-20°": 37},
                    "Freshwater Wetlands": {"Up slopes and flat": 19, ">0-5°": 22, ">5-10°": 25, ">10-15°": 28, ">15-20°": 30},
                    "Grassland": {"Up slopes and flat": 36, ">0-5°": 40, ">5-10°": 45, ">10-15°": 50, ">15-20°": 55},
                }

                def norm(t): return (t or "").strip().upper()
                
                def map_vegclass_to_keith(vegclass_text):
                    v = norm(vegclass_text)
                    if v == "NOT CLASSIFIED": return "Not classified"
                    if "RAINFOREST" in v: return "Rainforest"
                    if ("FOREST" in v and "WET" in v) or ("FOREST" in v and "DRY" in v) or ("SCLEROPHYLL" in v) or ("PINE" in v) or ("SUB-ALPINE" in v) or ("COASTAL SWAMP FOREST" in v): return "Forest (wet and dry sclerophyll) including Coastal Swamp Forest, Pine Plantations and Sub-Alpine Woodland"
                    if ("WOODLAND" in v and ("GRASSY" in v or "SEMI-ARID" in v)) or ("MALLEE" in v): return "Grassy and Semi-Arid Woodland (including Mallee)"
                    if ("WETLAND" in v and "FOREST" in v) or ("FORESTED WETLAND" in v):
                        if "COASTAL SWAMP FOREST" in v: return "Forest (wet and dry sclerophyll) including Coastal Swamp Forest, Pine Plantations and Sub-Alpine Woodland"
                        return "Forested Wetland (excluding Coastal Swamp Forest)"
                    if "TALL HEATH" in v: return "Tall Heath"
                    if "SHORT HEATH" in v or ("HEATH" in v and "SHORT" in v): return "Short Heath"
                    if "ARID" in v or "CHENOPOD" in v or ("ACACIA" in v and "SHRUB" in v): return "Arid-Shrublands (acacia and chenopod)"
                    if ("WETLAND" in v and "FRESHWATER" in v) or ("FRESHWATER" in v): return "Freshwater Wetlands"
                    if "GRASSLAND" in v: return "Grassland"
                    if "FOREST" in v: return "Forest (wet and dry sclerophyll) including Coastal Swamp Forest, Pine Plantations and Sub-Alpine Woodland"
                    if "WOODLAND" in v: return "Grassy and Semi-Arid Woodland (including Mallee)"
                    if "WETLAND" in v: return "Forested Wetland (excluding Coastal Swamp Forest)"
                    if "HEATH" in v: return "Short Heath"
                    return "Not classified"

                def effective_slope_value(slope_type, max_deg):
                    st = (slope_type or "").strip()
                    if st == "Up Slope": return "Up slopes and flat"
                    try: d = float(max_deg) if max_deg is not None else None
                    except: d = None
                    if d is None or d < 1: return "Up slopes and flat"
                    if d < 5: return ">0-5°"
                    if d < 10: return ">5-10°"
                    if d < 15: return ">10-15°"
                    return ">15-20°"

                _msg("Calculating Keith_Match, Effective_Slope, and APZ_Distance_M...")
                with arcpy.da.UpdateCursor(apz_assessment_fc, [vegclass_field, "Slope_Type", "SLOPE_MAX_DEG", "Keith_Match", "Effective_Slope", "APZ_Distance_M"]) as ucur:
                    for row in ucur:
                        vegclass_val, slope_type_val, max_slope_val = row[0], row[1], row[2]
                        
                        keith = map_vegclass_to_keith(vegclass_val)
                        row[3] = keith
                        
                        if norm(vegclass_val) == "NOT CLASSIFIED":
                            eff_slope = "N/A"
                        else:
                            eff_slope = effective_slope_value(slope_type_val, max_slope_val)
                        row[4] = eff_slope

                        apz_dist = None
                        if eff_slope == "N/A":
                            apz_dist = 36 # Minimum grassland distance rule
                        else:
                            apz_row_data = apz_table.get(keith)
                            if apz_row_data:
                                apz_dist = apz_row_data.get(eff_slope)
                        row[5] = int(apz_dist) if apz_dist is not None else None
                        
                        ucur.updateRow(row)
                _msg("APZ Assessment calculation complete.")

                # --- APZ VISUALIZATION PROTOCOL ---
                _msg("--- Initiating APZ Visualization Protocol ---")

                # Part 1: Generate Polyline Buffers
                _msg("-> Sub-protocol: APZ Polyline Generation.")
                apz_lines_name = f"AEP{project_number}_APZ_Buffer_Lines"
                apz_lines_fc = os.path.join(fds_path, apz_lines_name)
                apz_lines_fc = _prepare_output(apz_lines_fc, overwrite_outputs, "FeatureClass", workspace, geometry_type="POLYLINE", spatial_ref=sr)
                _msg(f"   - Schema defined. Target polyline feature class: {apz_lines_fc}")

                _msg("   - Appending attribute definitions to polyline schema: APZ_Distance_M, Keith_Match, Effective_Slope")
                arcpy.management.AddField(apz_lines_fc, "APZ_Distance_M", "LONG")
                arcpy.management.AddField(apz_lines_fc, "Keith_Match", "TEXT", field_length=120)
                arcpy.management.AddField(apz_lines_fc, "Effective_Slope", "TEXT", field_length=20)
                
                # Part 2: Prepare for Unified Polygon Buffer
                _msg("-> Sub-protocol: Unified APZ Polygon Generation.")
                apz_buffer_polygons_to_merge = []
                _msg("   - Transient polygon collection initialized.")

                _msg("-> Initiating iterative geoprocessing loop on assessment polygons...")
                fields_for_viz = ["SHAPE@", "APZ_Distance_M", "Keith_Match", "Effective_Slope"]
                with arcpy.da.SearchCursor(apz_assessment_fc, fields_for_viz) as scursor, \
                     arcpy.da.InsertCursor(apz_lines_fc, fields_for_viz) as icursor:
                    for i, row in enumerate(scursor):
                        poly_geom, apz_dist_val, keith_match_val, eff_slope_val = row
                        _msg(f"  > Processing polygon OID {i+1}: APZ Distance = {apz_dist_val}m.")
                        
                        if apz_dist_val is None or apz_dist_val <= 0:
                            _msg(f"    - Skipping: Invalid APZ distance ({apz_dist_val}).")
                            continue
                        
                        # In-memory names for this iteration
                        uuid_hex = uuid.uuid4().hex[:8]
                        mem_buffer_poly = f"in_memory\\buffer_poly_{uuid_hex}"
                        mem_clipped_poly = f"in_memory\\clipped_poly_{uuid_hex}"
                        mem_buffer_line = f"in_memory\\buffer_line_{uuid_hex}"
                        mem_clipped_line = f"in_memory\\clipped_line_{uuid_hex}"

                        _msg("    - Executing `Buffer` operation on source building geometry.")
                        arcpy.analysis.Buffer(building_fc, mem_buffer_poly, f"{apz_dist_val} Meters", "FULL", "ROUND", "NONE")
                        
                        # --- Logic for Unified Polygon ---
                        _msg("    - Executing `Clip` to isolate buffer POLYGON within assessment boundary.")
                        arcpy.analysis.Clip(mem_buffer_poly, poly_geom, mem_clipped_poly)
                        if int(arcpy.management.GetCount(mem_clipped_poly).getOutput(0)) > 0:
                            apz_buffer_polygons_to_merge.append(mem_clipped_poly)
                            _msg("    - Staging clipped polygon for final merge.")
                        
                        # --- Logic for Buffer Lines ---
                        _msg("    - Executing `PolygonToLine` conversion on buffer geometry.")
                        arcpy.management.PolygonToLine(mem_buffer_poly, mem_buffer_line, "IGNORE_NEIGHBORS")
                        
                        _msg("    - Executing `Clip` to isolate buffer LINE segment.")
                        arcpy.analysis.Clip(mem_buffer_line, poly_geom, mem_clipped_line)

                        if int(arcpy.management.GetCount(mem_clipped_line).getOutput(0)) > 0:
                            _msg("    - Appending clipped line segment to target feature class.")
                            with arcpy.da.SearchCursor(mem_clipped_line, ["SHAPE@"]) as line_cursor:
                                for line_row in line_cursor:
                                    icursor.insertRow([line_row[0], apz_dist_val, keith_match_val, eff_slope_val])
                        else:
                            _msg("    - Clip resulted in empty line feature; no segment to append.")
                        
                        _msg(f"    - Deallocating transient line resources for iteration {i+1}.")
                        for item in [mem_buffer_poly, mem_buffer_line, mem_clipped_line]:
                            if arcpy.Exists(item): arcpy.management.Delete(item)

                _msg("-> Iterative processing complete.")
                _msg("-> Polyline generation sub-protocol finished.")

                # Finalize Unified Polygon
                _msg("-> Executing geometry aggregation for unified APZ polygon.")
                if not apz_buffer_polygons_to_merge:
                    _warn("   - No clipped buffer polygons were generated; cannot create unified APZ layer.")
                else:
                    apz_poly_name = f"AEP{project_number}_APZ"
                    apz_poly_fc = os.path.join(fds_path, apz_poly_name)
                    _prepare_output(apz_poly_fc, overwrite_outputs, "FeatureClass", workspace, geometry_type="POLYGON", spatial_ref=sr)
                    
                    mem_merged = "in_memory\\merged_apz"
                    _msg("   - Executing `Merge` on transient clipped polygon collection.")
                    arcpy.management.Merge(apz_buffer_polygons_to_merge, mem_merged)
                    
                    _msg("   - Executing `Dissolve` to generate final unified APZ geometry.")
                    arcpy.management.Dissolve(mem_merged, apz_poly_fc)

                    _msg(f"   - Unified APZ polygon created: {apz_poly_fc}")
                    _msg("   - Deallocating transient polygon resources.")
                    arcpy.management.Delete(mem_merged)
                    for item in apz_buffer_polygons_to_merge:
                        if arcpy.Exists(item): arcpy.management.Delete(item)
                
                _msg("--- APZ Visualization Protocol Complete ---")

        except Exception as ex_viz:
            _warn(f"APZ Visualization Protocol failed: {ex_viz}")
            apz_lines_fc = None
            apz_poly_fc = None


        try:
            arcpy.management.Delete("in_memory")
        except Exception:
            pass

        if add_to_map:
            outputs_to_add = [
                apz_assessment_fc,
                apz_lines_fc,
                apz_poly_fc,
                svtm_site_slope_fc,
                bbuf_path,
                buffer_path,
                clipped_path,
                tin_path
            ]
            if bfpl_path and arcpy.Exists(bfpl_path):
                outputs_to_add.append(bfpl_path)
            if final_path and arcpy.Exists(final_path):
                outputs_to_add.append(final_path)
            self._add_outputs_to_map(outputs_to_add)
        else:
            _msg("Outputs not added to map (per parameter).")

        _msg("Process complete.")
        return

    def _infer_z_field(self, fc):
        _msg(f"Detecting elevation field in: {fc}")
        fields = [f for f in arcpy.ListFields(fc) if f.type in ("Integer", "SmallInteger", "Double", "Single")]
        candidates = ("ELEVATION", "ELEV", "Z", "CONTOUR", "VALUE")
        for cand in candidates:
            for f in fields:
                if f.name.upper() == cand:
                    _msg(f"Using elevation field: {f.name}")
                    return f.name
        if fields:
            _warn(f"No standard elevation field; defaulting to first numeric: {fields[0].name}")
            return fields[0].name
        raise arcpy.ExecuteError("No numeric elevation field found for contours.")

    def _add_outputs_to_map(self, paths):
        try:
            aprx = arcpy.mp.ArcGISProject("CURRENT")
            m = aprx.activeMap
            if not m:
                _warn("No active map found.")
                return
            for lyr_path in paths:
                try:
                    if not lyr_path or not arcpy.Exists(lyr_path):
                        continue
                    m.addDataFromPath(lyr_path)
                    _msg(f"Added to map: {lyr_path}")
                except Exception as ex_inner:
                    _warn(f"Add to map failed for '{lyr_path}': {ex_inner}")
            _msg("Map additions complete.")
        except Exception as ex:
            _warn(f"Map addition failed: {ex}")

    def _run_slope_analysis(self, in_tin, in_polygons, add_to_map, label=""):
        arcpy.env.overwriteOutput = True
        if not (in_tin and arcpy.Exists(in_tin)):
            raise arcpy.ExecuteError("Slope Analysis: TIN does not exist.")
        if not (in_polygons and arcpy.Exists(in_polygons)):
            raise arcpy.ExecuteError("Slope Analysis: polygons do not exist.")

        _msg(f"Slope analysis: {label}")

        try:
            desc = arcpy.Describe(in_tin)
            ds_type = getattr(desc, "datasetType", None) or getattr(desc, "dataType", None) or ""
            if ds_type is None or "tin" not in str(ds_type).lower():
                raise arcpy.ExecuteError(f"Slope Analysis: input is not a TIN (type='{ds_type}').")
        except arcpy.ExecuteError:
            raise
        except Exception as e:
            raise arcpy.ExecuteError(f"Slope Analysis: TIN validation failed: {e}")

        cell_size = 1
        target_sr = arcpy.SpatialReference(8058)
        aprx = None
        try:
            aprx = arcpy.mp.ArcGISProject("CURRENT")
            default_gdb = aprx.defaultGeodatabase
        except Exception:
            default_gdb = arcpy.env.workspace
            if not default_gdb:
                raise arcpy.ExecuteError("Slope Analysis: default geodatabase not found.")

        _msg(f"Output GDB: {default_gdb}")

        fd_name = "Slope"
        fd_path = os.path.join(default_gdb, fd_name)
        if not arcpy.Exists(fd_path):
            _msg(f"Creating feature dataset '{fd_name}' (EPSG:8058)")
            arcpy.CreateFeatureDataset_management(default_gdb, fd_name, target_sr)
        else:
            _msg(f"Feature dataset '{fd_name}' exists")

        in_polygons_name = arcpy.Describe(in_polygons).baseName
        out_fc_name = f"{in_polygons_name}_Slope"
        out_fc = os.path.join(fd_path, out_fc_name)

        guid = uuid.uuid4().hex[:8]
        mem = "in_memory"

        _msg("TIN to 1m raster...")
        tin_rast = os.path.join(mem, f"tin_rast_{guid}")
        try:
            arcpy.ddd.TinRaster(in_tin, tin_rast, "FLOAT", "#", "CELLSIZE", str(cell_size))
        except Exception:
            tin_rast = os.path.join(default_gdb, f"tin_rast_{guid}")
            arcpy.ddd.TinRaster(in_tin, tin_rast, "FLOAT", "#", "CELLSIZE", str(cell_size))

        _msg("Computing slope (deg) and aspect (deg)...")
        slope_rast = arcpy.sa.Slope(tin_rast, "DEGREE", z_factor=1)
        aspect_rast = arcpy.sa.Aspect(tin_rast)

        _msg("Preparing polygon output and ZoneID...")
        if arcpy.Exists(out_fc):
            arcpy.Delete_management(out_fc)
            _msg(f"Overwriting existing: {out_fc}")
        arcpy.management.CopyFeatures(in_polygons, out_fc)
        zone_field = "ZoneID"
        if zone_field not in [f.name for f in arcpy.ListFields(out_fc)]:
            arcpy.AddField_management(out_fc, zone_field, "LONG")
        oid_field = arcpy.Describe(out_fc).OIDFieldName
        arcpy.management.CalculateField(out_fc, zone_field, f"!{oid_field}!", "PYTHON3")

        _msg("Sampling rasters to points...")
        elev_pts = os.path.join(mem, f"elev_pts_{guid}")
        slope_pts = os.path.join(mem, f"slope_pts_{guid}")
        aspect_pts = os.path.join(mem, f"aspect_pts_{guid}")
        arcpy.conversion.RasterToPoint(tin_rast, elev_pts, "VALUE")
        arcpy.conversion.RasterToPoint(slope_rast, slope_pts, "VALUE")
        arcpy.conversion.RasterToPoint(aspect_rast, aspect_pts, "VALUE")

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
                    raise arcpy.ExecuteError(f"Value field not found in {pts}")
            if val_field != "VALUE":
                try:
                    arcpy.management.AlterField(pts, val_field, new_field_name="VALUE")
                except Exception:
                    arcpy.AddField_management(pts, "VALUE", "DOUBLE")
                    arcpy.management.CalculateField(pts, "VALUE", f"!{val_field}!", "PYTHON3")

        _msg("Spatial join points to polygons...")
        elev_pts_z = os.path.join(mem, f"elev_pts_z_{guid}")
        slope_pts_z = os.path.join(mem, f"slope_pts_z_{guid}")
        aspect_pts_z = os.path.join(mem, f"aspect_pts_z_{guid}")
        arcpy.analysis.SpatialJoin(elev_pts, out_fc, elev_pts_z, "JOIN_ONE_TO_ONE", "KEEP_COMMON", match_option="INTERSECT")
        arcpy.analysis.SpatialJoin(slope_pts, out_fc, slope_pts_z, "JOIN_ONE_TO_ONE", "KEEP_COMMON", match_option="INTERSECT")
        arcpy.analysis.SpatialJoin(aspect_pts, out_fc, aspect_pts_z, "JOIN_ONE_TO_ONE", "KEEP_COMMON", match_option="INTERSECT")

        def _find_zone_field(fc, zname="ZoneID"):
            for f in arcpy.ListFields(fc):
                if f.name.lower().startswith(zname.lower()):
                    return f.name
            return None

        zone_field_elev = _find_zone_field(elev_pts_z)
        zone_field_slope = _find_zone_field(slope_pts_z)
        zone_field_aspect = _find_zone_field(aspect_pts_z)
        if not zone_field_elev or not zone_field_slope or not zone_field_aspect:
            raise arcpy.ExecuteError("ZoneID field not found after spatial join.")

        _msg("Computing elevation and slope statistics...")
        elev_stats_tbl = os.path.join(mem, f"elev_stats_{guid}")
        slope_stats_tbl = os.path.join(mem, f"slope_stats_{guid}")
        stat_fields = [["VALUE", "MIN"], ["VALUE", "MAX"], ["VALUE", "MEAN"], ["VALUE", "STD"], ["VALUE", "MEDIAN"], ["VALUE", "COUNT"]]
        arcpy.analysis.Statistics(elev_pts_z, elev_stats_tbl, stat_fields, case_field=zone_field_elev)
        arcpy.analysis.Statistics(slope_pts_z, slope_stats_tbl, stat_fields, case_field=zone_field_slope)

        _msg("Computing circular statistics for aspect...")
        aspect_stats_tbl = os.path.join(mem, f"aspect_stats_{guid}")
        arcpy.management.CreateTable(mem, f"aspect_stats_{guid}")
        arcpy.AddField_management(aspect_stats_tbl, "ZoneID", "LONG")
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

        with arcpy.da.InsertCursor(aspect_stats_tbl, ["ZoneID", "ASPECT_MEAN_DEG", "ASPECT_STD_DEG", "ASPECT_SAMPLE_COUNT"]) as icur:
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

        _msg("Tabulating slope class areas...")
        class_definitions = {1: "0_5", 2: "5_15", 3: "15_30", 4: "30_45", 5: "45_plus"}
        remap = arcpy.sa.RemapRange([[0, 5, 1], [5, 15, 2], [15, 30, 3], [30, 45, 4], [45, 360, 5]])
        slope_class_rast = arcpy.sa.Reclassify(slope_rast, "VALUE", remap, "NODATA")
        class_rast_temp = os.path.join(mem, f"slope_class_{guid}")
        slope_class_rast.save(class_rast_temp)
        tab_area_tbl = os.path.join(mem, f"tab_area_{guid}")
        arcpy.sa.TabulateArea(out_fc, "ZoneID", class_rast_temp, "VALUE", tab_area_tbl, cell_size)

        _msg("Joining statistics and creating percentage fields...")
        elev_case_field = [f.name for f in arcpy.ListFields(elev_stats_tbl)][0]
        arcpy.management.JoinField(out_fc, "ZoneID", elev_stats_tbl, elev_case_field,
                                  ["MIN_VALUE", "MAX_VALUE", "MEAN_VALUE", "STD_VALUE", "MEDIAN_VALUE", "COUNT_VALUE"])
        self._rename_field_like(out_fc, "MIN_VALUE", "ELEV_MIN_M")
        self._rename_field_like(out_fc, "MAX_VALUE", "ELEV_MAX_M")
        self._rename_field_like(out_fc, "MEAN_VALUE", "ELEV_MEAN_M")
        self._rename_field_like(out_fc, "STD_VALUE", "ELEV_STD_M")
        self._rename_field_like(out_fc, "MEDIAN_VALUE", "ELEV_MEDIAN_M")
        self._rename_field_like(out_fc, "COUNT_VALUE", "ELEV_SAMPLE_COUNT")

        slope_case_field = [f.name for f in arcpy.ListFields(slope_stats_tbl)][0]
        arcpy.management.JoinField(out_fc, "ZoneID", slope_stats_tbl, slope_case_field,
                                  ["MIN_VALUE", "MAX_VALUE", "MEAN_VALUE", "STD_VALUE", "MEDIAN_VALUE", "COUNT_VALUE"])
        self._rename_field_like(out_fc, "MIN_VALUE", "SLOPE_MIN_DEG")
        self._rename_field_like(out_fc, "MAX_VALUE", "SLOPE_MAX_DEG")
        self._rename_field_like(out_fc, "MEAN_VALUE", "SLOPE_MEAN_DEG")
        self._rename_field_like(out_fc, "STD_VALUE", "SLOPE_STD_DEG")
        self._rename_field_like(out_fc, "MEDIAN_VALUE", "SLOPE_MEDIAN_DEG")
        self._rename_field_like(out_fc, "COUNT_VALUE", "SLOPE_SAMPLE_COUNT")

        arcpy.management.JoinField(out_fc, "ZoneID", aspect_stats_tbl, "ZoneID",
                                  ["ASPECT_MEAN_DEG", "ASPECT_STD_DEG", "ASPECT_SAMPLE_COUNT"])

        _msg(f"Slope analysis output: {out_fc}")
        
        try:
            for t in (elev_pts, slope_pts, aspect_pts, elev_pts_z, slope_pts_z, aspect_pts_z,
                      elev_stats_tbl, slope_stats_tbl, aspect_stats_tbl, class_rast_temp, tab_area_tbl, tin_rast):
                if t and arcpy.Exists(t):
                    arcpy.Delete_management(t)
        except Exception:
            pass

        _msg(f"Slope analysis complete: {label}")
        return out_fc

    def _calculate_polygon_area(self, fc, area_field, messages):
        if area_field not in [f.name for f in arcpy.ListFields(fc)]:
            arcpy.AddField_management(fc, area_field, "DOUBLE")
        tried = []
        try:
            _msg("Calculating geodesic area (m²)...")
            arcpy.management.CalculateGeometryAttributes(fc, [[area_field, "AREA_GEODESIC"]], area_unit="Square Meters")
            _msg(f"Geodesic area populated: {area_field}")
            return
        except Exception as e:
            tried.append(("AREA_GEODESIC", "Square Meters", str(e)))
        try:
            _msg("Calculating planar area (m²)...")
            arcpy.management.CalculateGeometryAttributes(fc, [[area_field, "AREA"]], area_unit="Square Meters")
            _msg(f"Planar area populated: {area_field}")
            return
        except Exception as e:
            tried.append(("AREA", "Square Meters", str(e)))
        alt_units = ["Square Meters", "SquareMeters", "SQUARE_METERS", "SQUAREMETERS", "Square Meters"]
        for alt in alt_units:
            try:
                _msg(f"Attempting planar area with unit '{alt}'...")
                arcpy.management.CalculateGeometryAttributes(fc, [[area_field, "AREA"]], area_unit=alt)
                _msg(f"Area populated using unit '{alt}': {area_field}")
                return
            except Exception as e:
                tried.append(("AREA", alt, str(e)))
                continue
        msg_lines = ["Polygon area calculation failed; attempts:"]
        for m, u, err in tried:
            msg_lines.append(f" - method: {m}, area_unit: {u}, error: {err}")
        raise arcpy.ExecuteError("\n".join(msg_lines))

    def _rename_field_like(self, fc, orig_prefix, newname):
        fields = [f.name for f in arcpy.ListFields(fc)]
        for fname in fields:
            if fname.lower().startswith(orig_prefix.lower()):
                if newname in fields:
                    return False
                try:
                    arcpy.management.AlterField(fc, fname, new_field_name=newname, new_field_alias=newname)
                    return True
                except Exception:
                    continue
        return False
