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
    _msg("STEP: Ensure standardized Feature Dataset (FD) exists for projected outputs.")
    _msg(f"-> Target Spatial Reference: EPSG:{TARGET_EPSG}")
    arcpy.env.workspace = workspace
    sr_target = arcpy.SpatialReference(TARGET_EPSG)

    def _fds_sr_ok(fds_path):
        try:
            _msg(f"   - Verifying Spatial Reference of existing FD: '{fds_path}'")
            desc = arcpy.Describe(fds_path)
            is_ok = desc.spatialReference.factoryCode == TARGET_EPSG
            _msg(f"   - FD CRS is EPSG:{desc.spatialReference.factoryCode}. Target matched: {is_ok}")
            return is_ok
        except Exception as ex:
            _warn(f"   - Describe failed for '{fds_path}': {ex}. Assuming it is not suitable.")
            return False

    fds_pref = os.path.join(workspace, FD_NAME)
    fds_alt = os.path.join(workspace, FD_ALT_NAME)
    _msg(f"-> Checking for preferred FD name: '{FD_NAME}'")
    if arcpy.Exists(fds_pref):
        if _fds_sr_ok(fds_pref):
            _msg(f"-> SUCCESS: Using existing and correctly projected FD: {fds_pref}")
            return fds_pref, sr_target
        else:
            _warn(f"-> WARNING: Preferred FD '{fds_pref}' exists but is not in EPSG:{TARGET_EPSG}. Will use alternate name to avoid conflicts.")

    _msg(f"-> Checking for alternate FD name: '{FD_ALT_NAME}'")
    if arcpy.Exists(fds_alt):
        if _fds_sr_ok(fds_alt):
            _msg(f"-> SUCCESS: Using existing alternate FD: {fds_alt}")
            return fds_alt, sr_target
        else:
            _warn(f"-> CRITICAL: Alternate FD '{fds_alt}' exists but is NOT in EPSG:{TARGET_EPSG}. This may cause errors. The script will proceed but results are not guaranteed.")
            return fds_alt, sr_target
    
    _msg(f"-> ACTION: Creating new Feature Dataset '{fds_alt}' with EPSG:{TARGET_EPSG}.")
    arcpy.management.CreateFeatureDataset(workspace, FD_ALT_NAME, sr_target)
    _msg("-> Feature Dataset creation complete.")
    return fds_alt, sr_target

def _delete_name_globally(gdb_workspace, name):
    _msg(f"STEP: Globally delete any item named '{name}' within the geodatabase.")
    prev = arcpy.env.workspace
    try:
        arcpy.env.workspace = gdb_workspace

        root_candidate = os.path.join(gdb_workspace, name)
        _msg(f"-> Checking for item at the GDB root: '{root_candidate}'")
        try:
            if arcpy.Exists(root_candidate):
                arcpy.management.Delete(root_candidate)
                _msg(f"   - DELETED root item: {root_candidate}")
        except Exception as ex_root:
            _warn(f"   - FAILED to delete root item '{root_candidate}': {ex_root}")

        _msg("-> Searching for matching Feature Classes at the GDB root...")
        try:
            for fc in arcpy.ListFeatureClasses(name):
                try:
                    arcpy.management.Delete(fc)
                    _msg(f"   - DELETED root Feature Class: {fc}")
                except Exception as ex_fc:
                    _warn(f"   - FAILED to delete root Feature Class '{fc}': {ex_fc}")
        except Exception as ex_list_root:
            _warn(f"   - FAILED to list root Feature Classes: {ex_list_root}")

        _msg("-> Searching for matching Rasters at the GDB root...")
        try:
            for ras in arcpy.ListRasters(name):
                try:
                    arcpy.management.Delete(ras)
                    _msg(f"   - DELETED root Raster: {ras}")
                except Exception as ex_r:
                    _warn(f"   - FAILED to delete root Raster '{ras}': {ex_r}")
        except Exception as ex_list_rast:
            _warn(f"   - FAILED to list root Rasters: {ex_list_rast}")

        _msg("-> Searching for matching items within all Feature Datasets...")
        try:
            for ds in arcpy.ListDatasets(feature_type='feature') or []:
                ds_path = os.path.join(gdb_workspace, ds)
                _msg(f"   - Scanning inside Dataset: '{ds}'")
                
                # Check for direct child
                candidate_in_ds = os.path.join(ds_path, name)
                try:
                    if arcpy.Exists(candidate_in_ds):
                        arcpy.management.Delete(candidate_in_ds)
                        _msg(f"     - DELETED direct child: {candidate_in_ds}")
                except Exception as ex_cds:
                    _warn(f"     - FAILED to delete direct child '{candidate_in_ds}': {ex_cds}")

                # List contents of dataset
                try:
                    arcpy.env.workspace = ds_path
                    for fc in arcpy.ListFeatureClasses(name):
                        try:
                            arcpy.management.Delete(fc)
                            _msg(f"     - DELETED Feature Class in Dataset '{ds}': {fc}")
                        except Exception as ex_fc_ds:
                            _warn(f"     - FAILED to delete Feature Class '{fc}' in Dataset '{ds}': {ex_fc_ds}")
                    for ras in arcpy.ListRasters(name):
                        try:
                            arcpy.management.Delete(ras)
                            _msg(f"     - DELETED Raster in Dataset '{ds}': {ras}")
                        except Exception as ex_rds:
                            _warn(f"     - FAILED to delete Raster '{ras}' in Dataset '{ds}': {ex_rds}")
                except Exception as ex_inner_ds:
                    _warn(f"     - FAILED to enumerate contents of Dataset '{ds}': {ex_inner_ds}")
        except Exception as ex_ds_list:
            _warn(f"   - FAILED to list Feature Datasets: {ex_ds_list}")

    finally:
        arcpy.env.workspace = prev
    _msg("-> Global delete sweep complete.")

def _unique_rename(path, data_type="FeatureClass"):
    if not arcpy.Exists(path):
        _msg(f"-> Path '{path}' is already unique. No rename needed.")
        return path
    
    _msg(f"-> Path '{path}' exists. Generating a new unique name.")
    stamp = datetime.now().strftime("%Y%m%d")
    base = os.path.basename(path)
    parent = os.path.dirname(path)
    candidate = f"{base}_{stamp}"
    candidate_path = os.path.join(parent, candidate)
    i = 1
    _msg(f"   - Attempting rename with base: {candidate}")
    while arcpy.Exists(candidate_path):
        candidate = f"{base}_{stamp}_{i}"
        candidate_path = os.path.join(parent, candidate)
        _msg(f"   - Name exists. Trying next sequential name: {candidate}")
        i += 1
    
    _msg(f"-> ACTION: Renaming existing data '{base}' to '{candidate}'.")
    arcpy.management.Rename(path, candidate, data_type)
    return candidate_path

def _prepare_output(path, overwrite, data_type="FeatureClass", gdb_workspace=None, geometry_type=None, spatial_ref=None):
    name = os.path.basename(path)
    _msg(f"STEP: Prepare output path for '{name}' (Type: {data_type})")
    
    if overwrite:
        _msg(f"-> Overwrite is ENABLED. Checking if '{path}' exists.")
        if arcpy.Exists(path):
            _msg(f"   - Item exists. Deleting '{name}'...")
            arcpy.management.Delete(path)
            _msg(f"   - Deletion successful.")
        else:
            _msg(f"   - Item does not exist. No deletion needed.")
    else:
        _msg(f"-> Overwrite is DISABLED. Ensuring unique name for '{name}'.")
        return _unique_rename(path, data_type)

    if not arcpy.Exists(path) and geometry_type:
        parent_dir = os.path.dirname(path)
        base_name = os.path.basename(path)
        _msg(f"-> ACTION: Creating new empty Feature Class '{base_name}' in '{parent_dir}'.")
        _msg(f"   - Geometry Type: {geometry_type}")
        _msg(f"   - Spatial Reference: {spatial_ref.name if spatial_ref else 'Default'}")
        arcpy.management.CreateFeatureclass(parent_dir, base_name, geometry_type, spatial_reference=spatial_ref)
        _msg("   - Feature Class created successfully.")
    
    return path

def _tin_output_path(workspace, tin_name):
    _msg(f"STEP: Determine output path for TIN '{tin_name}'.")
    if workspace.lower().endswith(".gdb"):
        base_folder = os.path.dirname(workspace)
        tin_folder = os.path.join(base_folder, "TINs")
        _msg(f"-> Workspace is a GDB. TIN will be stored outside, in: {tin_folder}")
    else:
        tin_folder = os.path.join(workspace, "TINs")
        _msg(f"-> Workspace is a folder. TIN will be stored inside, in: {tin_folder}")

    if not os.path.exists(tin_folder):
        _msg(f"-> ACTION: TIN output folder does not exist. Creating it now: {tin_folder}")
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
            "Generates buffers, clips contours/SVTM/BFPL, builds a TIN and DSM, classifies elevation (above/below threshold), overlays onto SVTM, and runs slope analysis. Calculates APZ and effectives[...]"
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
                _msg("UI BEHAVIOR: Populating 'Project Number' dropdown from live feature service.")
                _msg(f"-> Connecting to: {FEATURE_SERVICE_URL}")
                with arcpy.da.SearchCursor(FEATURE_SERVICE_URL, ["project_number"]) as cursor:
                    vals = sorted({row[0] for row in cursor if row[0]})
                parameters[1].filter.list = vals
                _msg(f"-> SUCCESS: Project number dropdown populated with {len(vals)} unique values.")
            except Exception as ex:
                _warn(f"-> WARNING: Failed to load project numbers from feature service: {ex}")
        return

    def execute(self, parameters, messages):
        _msg("=========================================================")
        _msg("START: Bushfire Preliminary Assessment (V10)")
        _msg("=========================================================")

        workspace = parameters[0].valueAsText
        project_number = parameters[1].valueAsText
        buffer_distance = float(parameters[2].value)
        contours_fc = parameters[3].valueAsText
        building_fc = parameters[4].valueAsText
        building_buffer_distance = float(parameters[5].value)
        split_elev = float(parameters[6].value)
        overwrite_outputs = bool(parameters[7].value)
        add_to_map = bool(parameters[8].value)
        
        _msg("STEP: Reading and validating user-provided parameters.")
        _msg(f"-> Output Workspace: {workspace}")
        _msg(f"-> Project Number: {project_number}")
        _msg(f"-> Site Buffer Distance: {buffer_distance} meters")
        _msg(f"-> Input 2m Contours: {contours_fc}")
        _msg(f"-> Input Building Outlines: {building_fc}")
        _msg(f"-> Building Buffer Distance: {building_buffer_distance} meters")
        _msg(f"-> Elevation Threshold for Classification: {split_elev} meters")
        _msg(f"-> Overwrite Existing Outputs: {overwrite_outputs}")
        _msg(f"-> Add Outputs to Map: {add_to_map}")

        # Validate building geometry
        _msg("STEP: Validate geometry type of Building Outlines layer.")
        try:
            bdesc = arcpy.Describe(building_fc)
            shape_type = getattr(bdesc, "shapeType", "").lower()
            _msg(f"-> Detected shape type: '{shape_type}'")
            if shape_type != "polygon":
                raise arcpy.ExecuteError(f"Input 'Building Outline' Feature Class must be of type Polygon, but found '{bdesc.shapeType}'.")
            _msg("-> Validation PASSED: Building Outlines are polygons.")
        except Exception as ex:
            raise arcpy.ExecuteError(f"CRITICAL ERROR: Validation failed for Building Outlines layer. Details: {ex}")

        fds_path, sr = _ensure_fds(workspace)

        _msg("STEP: Select Subject Site based on Project Number.")
        safe_project = project_number.replace("'", "''")
        where = f"project_number = '{safe_project}'"
        subject_layer = "subject_site_layer"
        _msg(f"-> Creating feature layer from: {FEATURE_SERVICE_URL}")
        _msg(f"-> Applying definition query (WHERE clause): {where}")
        arcpy.management.MakeFeatureLayer(FEATURE_SERVICE_URL, subject_layer, where)
        count = int(arcpy.management.GetCount(subject_layer).getOutput(0))
        _msg(f"-> Found {count} feature(s) matching the project number.")
        if count == 0:
            raise arcpy.ExecuteError(f"CRITICAL ERROR: No features found for project_number '{project_number}'. Please check the project number and try again.")

        _msg("STEP: Generate Site Buffer.")
        buffer_name = f"AEP{project_number}_Site_Buffer_{int(buffer_distance)}"
        buffer_path = os.path.join(fds_path, buffer_name)
        buffer_path = _prepare_output(buffer_path, overwrite_outputs, "FeatureClass", workspace)
        _msg(f"-> Buffering '{subject_layer}' by {buffer_distance} Meters.")
        _msg(f"-> Output will be saved to: {buffer_path}")
        arcpy.analysis.Buffer(subject_layer, buffer_path, f"{buffer_distance} Meters", dissolve_option="ALL")
        _msg("-> Site buffer created successfully.")

        _msg("STEP: Clip input 2m Contours to the Site Buffer extent.")
        clipped_name = f"AEP{project_number}_2m_Contours"
        clipped_path = os.path.join(fds_path, clipped_name)
        clipped_path = _prepare_output(clipped_path, overwrite_outputs, "FeatureClass", workspace)
        _msg(f"-> Clipping '{contours_fc}' using '{buffer_path}' as the clip feature.")
        _msg(f"-> Output will be saved to: {clipped_path}")
        arcpy.analysis.Clip(contours_fc, buffer_path, clipped_path)
        _msg("-> Contours clipped successfully.")

        _msg("STEP: Clip SVTM vegetation data to the Site Buffer extent.")
        svtm_name = f"AEP{project_number}_SVTM"
        svtm_path = os.path.join(fds_path, svtm_name)
        svtm_path = _prepare_output(svtm_path, overwrite_outputs, "FeatureClass", workspace)
        _msg(f"-> Creating feature layer from SVTM service: {SVTM_URL}")
        arcpy.management.MakeFeatureLayer(SVTM_URL, "svtm_layer")
        _msg(f"-> Clipping 'svtm_layer' using '{buffer_path}'.")
        _msg(f"-> Output will be saved to: {svtm_path}")
        arcpy.analysis.Clip("svtm_layer", buffer_path, svtm_path)
        _msg("-> SVTM vegetation data clipped successfully.")

        _msg("STEP: Clip BFPL fire prone land data to the Site Buffer extent.")
        bfpl_path = None
        try:
            bfpl_name = f"AEP{project_number}_BFPL"
            bfpl_path = os.path.join(fds_path, bfpl_name)
            bfpl_path = _prepare_output(bfpl_path, overwrite_outputs, "FeatureClass", workspace)
            _msg(f"-> Creating feature layer from BFPL service: {BFPL_URL}")
            arcpy.management.MakeFeatureLayer(BFPL_URL, "bfpl_layer")
            _msg(f"-> Clipping 'bfpl_layer' using '{buffer_path}'.")
            _msg(f"-> Output will be saved to: {bfpl_path}")
            arcpy.analysis.Clip("bfpl_layer", buffer_path, bfpl_path)
            _msg("-> BFPL data clipped successfully.")
        except Exception as ex_bfpl:
            _warn(f"-> WARNING: Clipping BFPL data failed. This step is non-critical. Processing will continue. Details: {ex_bfpl}")

        _msg("STEP: Generate Building Buffer.")
        bbuf_name = f"AEP{project_number}_Building_Buffer_{int(building_buffer_distance)}M"
        bbuf_path = os.path.join(fds_path, bbuf_name)
        bbuf_path = _prepare_output(bbuf_path, overwrite_outputs, "FeatureClass", workspace)
        _msg(f"-> Buffering '{building_fc}' by {building_buffer_distance} Meters.")
        _msg(f"-> Output will be saved to: {bbuf_path}")
        arcpy.analysis.Buffer(building_fc, bbuf_path, f"{building_buffer_distance} Meters", dissolve_option="ALL")
        _msg("-> Building buffer created successfully.")

        _msg("STEP: Clip SVTM data to the smaller Building Buffer extent.")
        svtm_bbuf_name = f"AEP{project_number}_SVTM_Bld_Buffer"
        svtm_bbuf_path = os.path.join(fds_path, svtm_bbuf_name)
        svtm_bbuf_path = _prepare_output(svtm_bbuf_path, overwrite_outputs, "FeatureClass", workspace)
        _msg(f"-> Clipping the site-level SVTM data ('{svtm_path}') using the building buffer ('{bbuf_path}').")
        _msg(f"-> Output will be saved to: {svtm_bbuf_path}")
        arcpy.analysis.Clip(svtm_path, bbuf_path, svtm_bbuf_path)
        _msg("-> SVTM clipped to building buffer successfully.")

        _msg("STEP: Erase building footprints from the SVTM building buffer data.")
        svtm_bbuf_erase_name = f"AEP{project_number}_SVTM_Bld_Buffer_NoBld"
        svtm_bbuf_erase_path = os.path.join(fds_path, svtm_bbuf_erase_name)
        svtm_bbuf_erase_path = _prepare_output(svtm_bbuf_erase_path, overwrite_outputs, "FeatureClass", workspace)
        _msg(f"-> Erasing '{building_fc}' from '{svtm_bbuf_path}'.")
        _msg(f"-> Output will be saved to: {svtm_bbuf_erase_path}")
        arcpy.analysis.Erase(svtm_bbuf_path, building_fc, svtm_bbuf_erase_path)
        _msg("-> Building footprints erased successfully, creating analysis-ready vegetation layer.")

        _msg("STEP: Generate TIN (Triangulated Irregular Network) from clipped contours.")
        tin_name = f"AEP{project_number}_TIN"
        tin_path = _tin_output_path(workspace, tin_name)
        if arcpy.Exists(tin_path):
            _msg(f"-> Found existing TIN at '{tin_path}'. Deleting to ensure fresh creation.")
            arcpy.management.Delete(tin_path)
        z_field = self._infer_z_field(clipped_path)
        _msg(f"-> Using field '{z_field}' from '{clipped_path}' as the height source for the TIN.")
        in_feats = [[clipped_path, z_field, "hardline"]]
        _msg(f"-> ACTION: Creating TIN surface. This may take some time...")
        _msg(f"   - Output TIN: {tin_path}")
        _msg(f"   - Spatial Reference: EPSG:{sr.factoryCode}")
        _msg(f"   - Input Features: {in_feats}")
        arcpy.ddd.CreateTin(out_tin=tin_path, spatial_reference=sr, in_features=in_feats, constrained_delaunay="DELAUNAY")
        _msg("-> TIN created successfully.")

        _msg("STEP: Generate raster DSM and classify elevation polygons.")
        _msg("-> Configuring raster analysis environment:")
        old_cell = arcpy.env.cellSize
        arcpy.env.mask = buffer_path
        arcpy.env.extent = buffer_path
        arcpy.env.cellSize = 1
        _msg(f"   - Cell Size: 1 meter")
        _msg(f"   - Processing Mask: '{buffer_path}'")
        _msg(f"   - Processing Extent: '{buffer_path}'")

        _msg("-> Sub-step: Convert TIN to a 1m resolution raster DSM.")
        dsm_tmp = os.path.join("in_memory", "dsm_1m")
        _msg(f"   - Creating temporary floating-point raster in memory: '{dsm_tmp}'")
        arcpy.ddd.TinRaster(tin_path, dsm_tmp, "FLOAT", "LINEAR", "CELLSIZE", 1)
        dsm_name = f"AEP{project_number}_DSM_1m"
        dsm_path = os.path.join(workspace, dsm_name)
        dsm_path = _prepare_output(dsm_path, overwrite_outputs, "RasterDataset", workspace)
        _msg(f"   - Copying temporary raster to permanent storage: '{dsm_path}'")
        arcpy.management.CopyRaster(dsm_tmp, dsm_path, pixel_type="32_BIT_FLOAT")
        _msg("-> DSM created successfully.")

        _msg("-> Sub-step: Convert DSM raster to polygons for classification.")
        _msg("   - Converting floating-point DSM to integer raster for polygon conversion.")
        dsm_int = arcpy.sa.Int(arcpy.sa.Raster(dsm_path))
        dsm_poly_name = f"AEP{project_number}_DSM_1m_Polys"
        dsm_poly_path = os.path.join(fds_path, dsm_poly_name)
        dsm_poly_path = _prepare_output(dsm_poly_path, overwrite_outputs, "FeatureClass", workspace)
        _msg(f"   - Converting integer raster to polygons: '{dsm_poly_path}'")
        arcpy.conversion.RasterToPolygon(dsm_int, dsm_poly_path, "SIMPLIFY")
        if arcpy.Exists(dsm_poly_path):
            _msg("   - Post-processing: Transferring elevation value from 'gridcode' to a new 'Elevation' field.")
            if "Elevation" not in [f.name for f in arcpy.ListFields(dsm_poly_path)]:
                arcpy.AddField_management(dsm_poly_path, "Elevation", "DOUBLE")
            with arcpy.da.UpdateCursor(dsm_poly_path, ["gridcode", "Elevation"]) as cur:
                for row in cur:
                    row[1] = float(row[0])
                    cur.updateRow(row)
        _msg("-> DSM polygons created and populated with elevation values.")

        _msg(f"-> Sub-step: Classify polygons as 'Greater' or 'LessEqual' than {split_elev} m elevation.")
        greater_tmp = os.path.join("in_memory", "dsm_greater")
        lesseq_tmp = os.path.join("in_memory", "dsm_lesseq")
        _msg(f"   - Selecting polygons with 'Elevation > {split_elev}'")
        arcpy.analysis.Select(dsm_poly_path, greater_tmp, f"Elevation > {split_elev}")
        _msg(f"   - Selecting polygons with 'Elevation <= {split_elev}'")
        arcpy.analysis.Select(dsm_poly_path, lesseq_tmp, f"Elevation <= {split_elev}")

        greater_diss = os.path.join("in_memory", "dsm_greater_diss")
        lesseq_diss = os.path.join("in_memory", "dsm_lesseq_diss")
        parts = []
        count_greater = int(arcpy.management.GetCount(greater_tmp).getOutput(0))
        _msg(f"   - Found {count_greater} polygons above the threshold.")
        if count_greater > 0:
            arcpy.management.Dissolve(greater_tmp, greater_diss)
            arcpy.AddField_management(greater_diss, "Relation", "TEXT", field_length=10)
            with arcpy.da.UpdateCursor(greater_diss, ["Relation"]) as cur:
                for row in cur: row[0] = "Greater"; cur.updateRow(row)
            parts.append(greater_diss)
        else:
            _warn("   - No polygons found above the elevation threshold.")

        count_lesseq = int(arcpy.management.GetCount(lesseq_tmp).getOutput(0))
        _msg(f"   - Found {count_lesseq} polygons at or below the threshold.")
        if count_lesseq > 0:
            arcpy.management.Dissolve(lesseq_tmp, lesseq_diss)
            arcpy.AddField_management(lesseq_diss, "Relation", "TEXT", field_length=10)
            with arcpy.da.UpdateCursor(lesseq_diss, ["Relation"]) as cur:
                for row in cur: row[0] = "LessEqual"; cur.updateRow(row)
            parts.append(lesseq_diss)
        else:
            _warn("   - No polygons found at or below the elevation threshold.")

        final_name = f"AEP{project_number}_Slope_Classification_{int(split_elev)}m"
        final_path = os.path.join(fds_path, final_name)
        final_path = _prepare_output(final_path, overwrite_outputs, "FeatureClass", workspace, geometry_type="POLYGON", spatial_ref=sr)
        if parts:
            _msg(f"-> Merging classified and dissolved polygons into final output: '{final_path}'")
            arcpy.management.Merge(parts, final_path)
            _msg("-> Merged. Now mapping 'Relation' field to final 'Slope_Type' field ('Up Slope'/'Down Slope').")
            
            if "Slope_Type" not in [f.name for f in arcpy.ListFields(final_path)]:
                arcpy.AddField_management(final_path, "Slope_Type", "TEXT", field_length=30)
            
            with arcpy.da.UpdateCursor(final_path, ["Relation", "Slope_Type"]) as ucur_final:
                for urow in ucur_final:
                    rel = (urow[0] or "").strip()
                    if rel == "LessEqual": urow[1] = "Up Slope"
                    elif rel == "Greater": urow[1] = "Down Slope"
                    else: urow[1] = rel if rel else None
                    ucur_final.updateRow(urow)
            
            try:
                arcpy.management.DeleteField(final_path, "Relation")
                _msg("-> 'Relation' field successfully mapped and deleted.")
            except Exception as ex_del_rel_final:
                _warn(f"-> Could not delete temporary 'Relation' field: {ex_del_rel_final}")
        else:
            _warn("-> No classification polygons were generated to merge. The final classification layer will be empty.")
        _msg("-> Elevation classification complete.")

        _msg("STEP: Overlay Slope Classification onto SVTM Layers.")
        overlay_ok = True
        try:
            if not final_path or not arcpy.Exists(final_path):
                _warn("-> Classification layer is missing or empty; cannot perform SVTM overlay. Skipping.")
                overlay_ok = False
            else:
                _msg("-> Preparing to overlay slope classification onto two SVTM variants.")
                svtm_variants = [
                    ("SVTM (site)", svtm_path),
                    ("SVTM Building Buffer No Building", svtm_bbuf_erase_path)
                ]
                for label, svtm_fc in svtm_variants:
                    if not svtm_fc or not arcpy.Exists(svtm_fc):
                        _warn(f"-> SKIPPING OVERLAY for '{label}': source feature class is missing ({svtm_fc}).")
                        overlay_ok = False
                        continue

                    _msg(f"-> Processing overlay for: '{label}'")
                    temp_svtm = os.path.join("in_memory", f"svtm_temp_{datetime.now().strftime('%H%M%S')}")
                    ident_tmp = os.path.join("in_memory", f"svtm_ident_{datetime.now().strftime('%H%M%S')}")

                    _msg("   - Making an in-memory copy to avoid altering original until success.")
                    arcpy.management.CopyFeatures(svtm_fc, temp_svtm)
                    
                    if "Relation" in [f.name for f in arcpy.ListFields(temp_svtm)]:
                        _msg("   - Cleaning up pre-existing 'Relation' field from copied data.")
                        try: arcpy.management.DeleteField(temp_svtm, "Relation")
                        except Exception as ex_del: _warn(f"   - Could not delete 'Relation' field: {ex_del}")

                    _msg(f"   - Running Identity tool with classification layer '{final_path}'.")
                    arcpy.analysis.Identity(temp_svtm, final_path, ident_tmp)
                    
                    _msg("   - Overlay successful. Replacing original feature class with updated data.")
                    try:
                        if arcpy.Exists(svtm_fc): arcpy.management.Delete(svtm_fc)
                        arcpy.management.CopyFeatures(ident_tmp, svtm_fc)

                        _msg("   - Mapping 'Relation' to 'Slope_Type' on the new overlay layer.")
                        fields_after = [f.name for f in arcpy.ListFields(svtm_fc)]
                        if "Slope_Type" not in fields_after and "Relation" in fields_after:
                            arcpy.AddField_management(svtm_fc, "Slope_Type", "TEXT", field_length=30)
                            with arcpy.da.UpdateCursor(svtm_fc, ["Relation", "Slope_Type"]) as ucur:
                                for urow in ucur:
                                    rel = (urow[0] or "").strip()
                                    if rel == "LessEqual": urow[1] = "Up Slope"
                                    elif rel == "Greater": urow[1] = "Down Slope"
                                    else: urow[1] = rel if rel else None
                                    ucur.updateRow(urow)
                            try:
                                arcpy.management.DeleteField(svtm_fc, "Relation")
                                _msg("   - 'Relation' field mapped and deleted successfully.")
                            except Exception as ex_del_rel:
                                _warn(f"   - Could not delete 'Relation' field after mapping: {ex_del_rel}")
                        _msg(f"-> Overlay complete for '{label}'.")
                    except Exception as ex_copy:
                        overlay_ok = False
                        _warn(f"-> CRITICAL: Failed to copy Identity result back to '{svtm_fc}'. The layer may be corrupt or missing. Details: {ex_copy}")
        except Exception as ex_ident:
            overlay_ok = False
            _warn(f"-> CRITICAL: An unexpected error occurred during the SVTM overlay process. Details: {ex_ident}")

        _msg("STEP: Restore geoprocessing environment.")
        arcpy.env.cellSize = old_cell
        arcpy.env.mask = None
        arcpy.env.extent = None
        arcpy.env.outputCoordinateSystem = None
        _msg("-> Environment settings (Cell Size, Mask, Extent) restored to their original values.")

        _msg("STEP: Perform detailed Slope Analysis on SVTM layers.")
        svtm_site_slope_fc = None
        svtm_bbuf_slope_fc = None
        if overlay_ok and final_path and arcpy.Exists(final_path):
            try:
                _msg("-> Running slope analysis for: SVTM (full site).")
                svtm_site_slope_fc = self._run_slope_analysis(in_tin=tin_path, in_polygons=svtm_path, add_to_map=False, label="SVTM (site)")
            except Exception as ex:
                _warn(f"-> WARNING: Slope analysis failed for 'SVTM (site)'. Details: {ex}")
            try:
                _msg("-> Running slope analysis for: SVTM Building Buffer (No Buildings).")
                svtm_bbuf_slope_fc = self._run_slope_analysis(in_tin=tin_path, in_polygons=svtm_bbuf_erase_path, add_to_map=False, label="SVTM Building Buffer No Building")
            except Exception as ex:
                _warn(f"-> WARNING: Slope analysis failed for 'SVTM Building Buffer No Building'. Details: {ex}")
        else:
            _warn("-> SKIPPING slope analysis because the prior overlay step failed or the classification layer is missing.")

        _msg("STEP: Perform APZ Assessment and Visualization.")
        apz_assessment_fc = None
        apz_lines_fc = None
        apz_poly_fc = None
        try:
            target_fc = svtm_bbuf_slope_fc
            if not (target_fc and arcpy.Exists(target_fc)):
                _warn("-> SKIPPING APZ Assessment: Prerequisite slope analysis output is missing.")
            else:
                _msg("--- Initiating APZ Assessment Protocol ---")
                
                apz_assessment_name = f"AEP{project_number}_APZ_Assessment"
                apz_assessment_fc = os.path.join(fds_path, apz_assessment_name)
                apz_assessment_fc = _prepare_output(apz_assessment_fc, overwrite_outputs, "FeatureClass", workspace, geometry_type="POLYGON", spatial_ref=sr)
                _msg(f"-> Creating a copy of the slope analysis output for assessment: {apz_assessment_fc}")
                arcpy.management.CopyFeatures(target_fc, apz_assessment_fc)

                _msg("-> Preparing schema for assessment results...")
                field_names = [f.name for f in arcpy.ListFields(apz_assessment_fc)]
                if "Keith_Match" not in field_names: arcpy.AddField_management(apz_assessment_fc, "Keith_Match", "TEXT", field_length=120)
                if "Effective_Slope" not in field_names: arcpy.AddField_management(apz_assessment_fc, "Effective_Slope", "TEXT", field_length=20)
                if "APZ_Distance_M" not in field_names: arcpy.AddField_management(apz_assessment_fc, "APZ_Distance_M", "LONG")
                _msg("   - Fields 'Keith_Match', 'Effective_Slope', 'APZ_Distance_M' are present.")

                vegclass_field = next((cand for cand in ("vegClass", "VegClass", "VEGCLASS") if cand in field_names), None)
                if not vegclass_field: raise arcpy.ExecuteError("CRITICAL: 'vegClass' field not found in the input data for APZ assessment.")
                _msg(f"-> Found vegetation classification field: '{vegclass_field}'")

                apz_table = {
                    "Rainforest": {"Up slopes and flat": 38, ">0-5°": 47, ">5-10°": 57, ">10-15°": 69, ">15-20°": 81},
                    "Forest (wet and dry sclerophyll) including Coastal Swamp Forest, Pine Plantations and Sub-Alpine Woodland": {"Up slopes and flat": 67, ">0-5°": 79, ">5-10°": 93, ">10-15°": 100},
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

                _msg("-> ACTION: Calculating APZ distances based on vegetation and slope...")
                with arcpy.da.UpdateCursor(apz_assessment_fc, [vegclass_field, "Slope_Type", "SLOPE_MAX_DEG", "Keith_Match", "Effective_Slope", "APZ_Distance_M"]) as ucur:
                    for row in ucur:
                        vegclass_val, slope_type_val, max_slope_val = row[0], row[1], row[2]
                        
                        keith = map_vegclass_to_keith(vegclass_val)
                        row[3] = keith
                        
                        eff_slope = "N/A" if norm(vegclass_val) == "NOT CLASSIFIED" else effective_slope_value(slope_type_val, max_slope_val)
                        row[4] = eff_slope

                        apz_dist = None
                        if eff_slope == "N/A":
                            apz_dist = 36 # Minimum grassland distance rule
                        else:
                            apz_row_data = apz_table.get(keith)
                            if apz_row_data: apz_dist = apz_row_data.get(eff_slope)
                        row[5] = int(apz_dist) if apz_dist is not None else None
                        
                        ucur.updateRow(row)
                _msg("-> APZ Assessment calculation complete. All polygons now have APZ distances.")

                _msg("--- Initiating APZ Visualization Protocol ---")
                _msg("-> Sub-protocol: Generate APZ Buffer Polyline segments.")
                apz_lines_name = f"AEP{project_number}_APZ_Buffer_Lines"
                apz_lines_fc = os.path.join(fds_path, apz_lines_name)
                apz_lines_fc = _prepare_output(apz_lines_fc, overwrite_outputs, "FeatureClass", workspace, geometry_type="POLYLINE", spatial_ref=sr)
                _msg(f"   - Target polyline feature class created: {apz_lines_fc}")

                # --- ROBUSTNESS PATCH: Ensure the APZ polyline FC actually exists before adding fields ---
                if not arcpy.Exists(apz_lines_fc):
                    _warn(f"-> WARNING: APZ polyline target was not found after _prepare_output. Explicitly creating now: {apz_lines_fc}")
                    parent_dir = os.path.dirname(apz_lines_fc)
                    base_name = os.path.basename(apz_lines_fc)
                    try:
                        arcpy.management.CreateFeatureclass(parent_dir, base_name, "POLYLINE", spatial_reference=sr)
                        _msg(f"   - Successfully created missing APZ polyline feature class: {base_name}")
                    except Exception as ex_create_apz:
                        raise arcpy.ExecuteError(f"CRITICAL ERROR: Failed to create required APZ polyline feature class '{apz_lines_fc}'. Cannot proceed. Details: {ex_create_apz}")

                _msg("   - Appending attribute definitions to polyline schema: APZ_Distance_M, Keith_Match, Effective_Slope")
                arcpy.management.AddField(apz_lines_fc, "APZ_Distance_M", "LONG")
                arcpy.management.AddField(apz_lines_fc, "Keith_Match", "TEXT", field_length=120)
                arcpy.management.AddField(apz_lines_fc, "Effective_Slope", "TEXT", field_length=20)
                
                _msg("-> Sub-protocol: Prepare for Unified APZ Polygon generation.")
                apz_buffer_polygons_to_merge = []
                _msg("   - In-memory collection for transient polygons initialized.")

                _msg("-> ACTION: Initiating loop through assessment polygons to generate visualization geometries.")
                fields_for_viz = ["SHAPE@", "APZ_Distance_M", "Keith_Match", "Effective_Slope", "OID@"]
                with arcpy.da.SearchCursor(apz_assessment_fc, fields_for_viz) as scursor, \
                     arcpy.da.InsertCursor(apz_lines_fc, ["SHAPE@", "APZ_Distance_M", "Keith_Match", "Effective_Slope"]) as icursor:
                    for row in scursor:
                        poly_geom, apz_dist_val, keith_match_val, eff_slope_val, oid = row
                        _msg(f"  > Processing polygon OID {oid}: APZ Distance = {apz_dist_val}m.")
                        
                        if apz_dist_val is None or apz_dist_val <= 0:
                            _msg(f"    - SKIPPING: Invalid APZ distance ({apz_dist_val}).")
                            continue
                        
                        uuid_hex = uuid.uuid4().hex[:8]
                        mem_buffer_poly = f"in_memory\\buffer_poly_{uuid_hex}"
                        mem_clipped_poly = f"in_memory\\clipped_poly_{uuid_hex}"
                        mem_buffer_line = f"in_memory\\buffer_line_{uuid_hex}"
                        mem_clipped_line = f"in_memory\\clipped_line_{uuid_hex}"

                        _msg("    - Buffering building footprints by APZ distance.")
                        arcpy.analysis.Buffer(building_fc, mem_buffer_poly, f"{apz_dist_val} Meters", "FULL", "ROUND", "NONE")
                        
                        _msg("    - Clipping the full buffer POLYGON to the current assessment polygon's boundary.")
                        arcpy.analysis.Clip(mem_buffer_poly, poly_geom, mem_clipped_poly)
                        if int(arcpy.management.GetCount(mem_clipped_poly).getOutput(0)) > 0:
                            apz_buffer_polygons_to_merge.append(mem_clipped_poly)
                            _msg("    - Staging the clipped polygon piece for final merge.")
                        
                        _msg("    - Converting buffer polygon to polylines.")
                        arcpy.management.PolygonToLine(mem_buffer_poly, mem_buffer_line, "IGNORE_NEIGHBORS")
                        
                        _msg("    - Clipping buffer LINE to the current assessment polygon's boundary.")
                        arcpy.analysis.Clip(mem_buffer_line, poly_geom, mem_clipped_line)

                        if int(arcpy.management.GetCount(mem_clipped_line).getOutput(0)) > 0:
                            _msg("    - Appending resulting line segments to the final polyline layer.")
                            with arcpy.da.SearchCursor(mem_clipped_line, ["SHAPE@"]) as line_cursor:
                                for line_row in line_cursor:
                                    icursor.insertRow([line_row[0], apz_dist_val, keith_match_val, eff_slope_val])
                        else:
                            _msg("    - Clip resulted in empty line feature; no segment to append.")
                        
                        for item in [mem_buffer_poly, mem_buffer_line, mem_clipped_line]:
                            if arcpy.Exists(item): arcpy.management.Delete(item)

                _msg("-> Iterative processing complete. All assessment polygons have been processed.")
                _msg("-> Polyline generation sub-protocol finished.")

                _msg("-> Finalizing Unified APZ Polygon.")
                if not apz_buffer_polygons_to_merge:
                    _warn("   - WARNING: No clipped buffer polygons were generated. Cannot create a unified APZ polygon layer.")
                else:
                    apz_poly_name = f"AEP{project_number}_APZ"
                    apz_poly_fc = os.path.join(fds_path, apz_poly_name)
                    _prepare_output(apz_poly_fc, overwrite_outputs, "FeatureClass", workspace, geometry_type="POLYGON", spatial_ref=sr)
                    
                    mem_merged = "in_memory\\merged_apz"
                    _msg(f"   - Merging {len(apz_buffer_polygons_to_merge)} transient clipped polygons.")
                    arcpy.management.Merge(apz_buffer_polygons_to_merge, mem_merged)
                    
                    _msg("   - Dissolving merged polygons to create final seamless APZ geometry.")
                    arcpy.management.Dissolve(mem_merged, apz_poly_fc)

                    _msg(f"   - SUCCESS: Unified APZ polygon created: {apz_poly_fc}")
                    _msg("   - Deallocating transient polygon resources.")
                    arcpy.management.Delete(mem_merged)
                    for item in apz_buffer_polygons_to_merge:
                        if arcpy.Exists(item): arcpy.management.Delete(item)
                
                _msg("--- APZ Visualization Protocol Complete ---")

        except Exception as ex_viz:
            _warn(f"-> CRITICAL WARNING: The APZ Assessment and Visualization Protocol failed unexpectedly. Details: {ex_viz}")
            apz_lines_fc = None
            apz_poly_fc = None

        _msg("STEP: Final Cleanup.")
        try:
            _msg("-> Deleting all items from 'in_memory' workspace.")
            arcpy.management.Delete("in_memory")
            _msg("-> In-memory workspace cleared.")
        except Exception:
            _warn("-> Notice: Could not clear 'in_memory' workspace. This is usually not a problem.")

        if add_to_map:
            _msg("STEP: Add specified outputs to the current map.")
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
            if bfpl_path and arcpy.Exists(bfpl_path): outputs_to_add.append(bfpl_path)
            if final_path and arcpy.Exists(final_path): outputs_to_add.append(final_path)
            self._add_outputs_to_map(outputs_to_add)
        else:
            _msg("STEP: Add outputs to map was skipped by user parameter.")

        _msg("=========================================================")
        _msg("FINISH: Bushfire Preliminary Assessment (V10) Complete.")
        _msg("=========================================================")
        return

    def _infer_z_field(self, fc):
        _msg(f"-> Utility: Detecting elevation field in '{fc}'")
        fields = [f for f in arcpy.ListFields(fc) if f.type in ("Integer", "SmallInteger", "Double", "Single")]
        candidates = ("ELEVATION", "ELEV", "Z", "CONTOUR", "VALUE")
        _msg(f"   - Searching for standard names: {candidates}")
        for cand in candidates:
            for f in fields:
                if f.name.upper() == cand:
                    _msg(f"   - Found standard elevation field: '{f.name}'")
                    return f.name
        if fields:
            _warn(f"   - No standard elevation field found. Defaulting to first available numeric field: '{fields[0].name}'")
            return fields[0].name
        raise arcpy.ExecuteError(f"CRITICAL: No numeric field found in '{fc}' to use for elevation.")

    def _add_outputs_to_map(self, paths):
        _msg("-> Attempting to add final layers to the active map view.")
        try:
            aprx = arcpy.mp.ArcGISProject("CURRENT")
            m = aprx.activeMap
            if not m:
                _warn("-> No active map found. Cannot add layers.")
                return
            _msg(f"-> Found active map: '{m.name}'")
            for lyr_path in paths:
                try:
                    if not lyr_path or not arcpy.Exists(lyr_path):
                        _msg(f"   - Skipping non-existent layer: '{lyr_path}'")
                        continue
                    m.addDataFromPath(lyr_path)
                    _msg(f"   - ADDED: {os.path.basename(lyr_path)}")
                except Exception as ex_inner:
                    _warn(f"   - FAILED to add layer '{lyr_path}': {ex_inner}")
            _msg("-> Map addition process complete.")
        except Exception as ex:
            _warn(f"-> An error occurred while trying to access the current ArcGIS Pro project: {ex}")

    def _run_slope_analysis(self, in_tin, in_polygons, add_to_map, label=""):
        _msg(f"--- Sub-routine: Slope Analysis for '{label}' ---")
        arcpy.env.overwriteOutput = True
        if not (in_tin and arcpy.Exists(in_tin)): raise arcpy.ExecuteError("Slope Analysis: Input TIN does not exist.")
        if not (in_polygons and arcpy.Exists(in_polygons)): raise arcpy.ExecuteError("Slope Analysis: Input polygons do not exist.")

        _msg("-> Validating inputs...")
        try:
            desc = arcpy.Describe(in_tin)
            ds_type = getattr(desc, "datasetType", None) or getattr(desc, "dataType", None) or ""
            if "tin" not in str(ds_type).lower(): raise arcpy.ExecuteError(f"Input is not a TIN (type='{ds_type}').")
            _msg("   - Input TIN is valid.")
        except arcpy.ExecuteError: raise
        except Exception as e: raise arcpy.ExecuteError(f"TIN validation failed: {e}")

        cell_size = 1
        target_sr = arcpy.SpatialReference(8058)
        try: aprx = arcpy.mp.ArcGISProject("CURRENT"); default_gdb = aprx.defaultGeodatabase
        except Exception: default_gdb = arcpy.env.workspace
        if not default_gdb: raise arcpy.ExecuteError("Slope Analysis: Could not determine default geodatabase.")
        _msg(f"-> Output will be stored in: {default_gdb}")

        fd_name = "Slope"
        fd_path = os.path.join(default_gdb, fd_name)
        if not arcpy.Exists(fd_path):
            _msg(f"-> Creating 'Slope' feature dataset (EPSG:{target_sr.factoryCode}).")
            arcpy.CreateFeatureDataset_management(default_gdb, fd_name, target_sr)
        
        in_polygons_name = arcpy.Describe(in_polygons).baseName
        out_fc_name = f"{in_polygons_name}_Slope"
        out_fc = os.path.join(fd_path, out_fc_name)
        _msg(f"-> Final output Feature Class will be: {out_fc}")
        
        guid = uuid.uuid4().hex[:8]
        mem = "in_memory"

        _msg("-> Converting TIN to a 1m temporary raster...")
        tin_rast = os.path.join(mem, f"tin_rast_{guid}")
        try: arcpy.ddd.TinRaster(in_tin, tin_rast, "FLOAT", "#", "CELLSIZE", str(cell_size))
        except Exception:
            _warn("   - In-memory TIN raster failed, trying to write to GDB instead.")
            tin_rast = os.path.join(default_gdb, f"tin_rast_{guid}")
            arcpy.ddd.TinRaster(in_tin, tin_rast, "FLOAT", "#", "CELLSIZE", str(cell_size))

        _msg("-> Computing Slope (degrees) and Aspect (degrees) from TIN raster.")
        slope_rast = arcpy.sa.Slope(tin_rast, "DEGREE", z_factor=1)
        aspect_rast = arcpy.sa.Aspect(tin_rast)

        _msg("-> Preparing output polygon layer and unique ZoneID for statistics.")
        if arcpy.Exists(out_fc):
            arcpy.Delete_management(out_fc)
            _msg(f"   - Overwriting existing output: {out_fc}")
        arcpy.management.CopyFeatures(in_polygons, out_fc)
        zone_field = "ZoneID"
        if zone_field not in [f.name for f in arcpy.ListFields(out_fc)]:
            arcpy.AddField_management(out_fc, zone_field, "LONG")
        oid_field = arcpy.Describe(out_fc).OIDFieldName
        arcpy.management.CalculateField(out_fc, zone_field, f"!{oid_field}!", "PYTHON3")
        _msg("   - 'ZoneID' populated.")

        _msg("-> Sampling raster values to points for zonal statistics...")
        elev_pts = os.path.join(mem, f"elev_pts_{guid}")
        slope_pts = os.path.join(mem, f"slope_pts_{guid}")
        aspect_pts = os.path.join(mem, f"aspect_pts_{guid}")
        arcpy.conversion.RasterToPoint(tin_rast, elev_pts, "VALUE")
        arcpy.conversion.RasterToPoint(slope_rast, slope_pts, "VALUE")
        arcpy.conversion.RasterToPoint(aspect_rast, aspect_pts, "VALUE")
        _msg("   - Raster to Point conversion complete for elevation, slope, and aspect.")

        for pts in (elev_pts, slope_pts, aspect_pts): # Standardize value field name
            val_field = "grid_code"
            if val_field != "VALUE":
                try: arcpy.management.AlterField(pts, val_field, new_field_name="VALUE")
                except:
                    arcpy.AddField_management(pts, "VALUE", "DOUBLE")
                    arcpy.management.CalculateField(pts, "VALUE", f"!{val_field}!", "PYTHON3")
        _msg("   - Standardized value field to 'VALUE' for all point layers.")
        
        _msg("-> Spatially joining points to polygons to assign ZoneID to each point...")
        elev_pts_z = os.path.join(mem, f"elev_pts_z_{guid}")
        slope_pts_z = os.path.join(mem, f"slope_pts_z_{guid}")
        aspect_pts_z = os.path.join(mem, f"aspect_pts_z_{guid}")
        arcpy.analysis.SpatialJoin(elev_pts, out_fc, elev_pts_z, "JOIN_ONE_TO_ONE", "KEEP_COMMON", match_option="INTERSECT")
        arcpy.analysis.SpatialJoin(slope_pts, out_fc, slope_pts_z, "JOIN_ONE_TO_ONE", "KEEP_COMMON", match_option="INTERSECT")
        arcpy.analysis.SpatialJoin(aspect_pts, out_fc, aspect_pts_z, "JOIN_ONE_TO_ONE", "KEEP_COMMON", match_option="INTERSECT")
        _msg("   - Spatial joins complete.")

        def _find_zone_field(fc, zname="ZoneID"):
            fn = [f.name for f in arcpy.ListFields(fc) if f.name.lower().startswith(zname.lower())]
            return fn[0] if fn else None

        zone_field_elev = _find_zone_field(elev_pts_z)
        zone_field_slope = _find_zone_field(slope_pts_z)
        zone_field_aspect = _find_zone_field(aspect_pts_z)
        if not all([zone_field_elev, zone_field_slope, zone_field_aspect]): raise arcpy.ExecuteError("ZoneID field not found after spatial join.")

        _msg("-> Computing standard statistics (MIN, MAX, MEAN, STD, MEDIAN) for Elevation and Slope.")
        elev_stats_tbl = os.path.join(mem, f"elev_stats_{guid}")
        slope_stats_tbl = os.path.join(mem, f"slope_stats_{guid}")
        stat_fields = [["VALUE", "MIN"], ["VALUE", "MAX"], ["VALUE", "MEAN"], ["VALUE", "STD"], ["VALUE", "MEDIAN"], ["VALUE", "COUNT"]]
        arcpy.analysis.Statistics(elev_pts_z, elev_stats_tbl, stat_fields, case_field=zone_field_elev)
        arcpy.analysis.Statistics(slope_pts_z, slope_stats_tbl, stat_fields, case_field=zone_field_slope)
        _msg("   - Statistics tables generated.")

        _msg("-> Computing circular statistics (MEAN, STD) for Aspect.")
        aspect_stats_tbl = os.path.join(mem, f"aspect_stats_{guid}")
        arcpy.management.CreateTable(mem, f"aspect_stats_{guid}")
        arcpy.AddField_management(aspect_stats_tbl, "ZoneID", "LONG"); arcpy.AddField_management(aspect_stats_tbl, "ASPECT_MEAN_DEG", "DOUBLE")
        arcpy.AddField_management(aspect_stats_tbl, "ASPECT_STD_DEG", "DOUBLE"); arcpy.AddField_management(aspect_stats_tbl, "ASPECT_SAMPLE_COUNT", "LONG")
        
        sums, counts = {}, {}
        for row in arcpy.da.SearchCursor(aspect_pts_z, [zone_field_aspect, "VALUE"]):
            z, v = row[0], row[1]
            if v is None or float(v) < 0: continue
            rad = math.radians(float(v)); cs = math.cos(rad); sn = math.sin(rad)
            if z in sums: sums[z][0] += cs; sums[z][1] += sn; counts[z] += 1
            else: sums[z] = [cs, sn]; counts[z] = 1

        with arcpy.da.InsertCursor(aspect_stats_tbl, ["ZoneID", "ASPECT_MEAN_DEG", "ASPECT_STD_DEG", "ASPECT_SAMPLE_COUNT"]) as icur:
            for z, (sum_cos, sum_sin) in sums.items():
                n = counts.get(z, 0)
                if n == 0: continue
                avg_cos = sum_cos / n; avg_sin = sum_sin / n
                mean_rad = math.atan2(avg_sin, avg_cos); mean_deg = math.degrees(mean_rad)
                if mean_deg < 0: mean_deg += 360.0
                R = math.sqrt(avg_cos**2 + avg_sin**2)
                try: circ_std = math.sqrt(-2.0 * math.log(max(min(R, 1.0), 1e-12)))
                except: circ_std = 0.0
                circ_std_deg = math.degrees(circ_std)
                icur.insertRow((z, mean_deg, circ_std_deg, n))
        _msg("   - Circular statistics calculated and stored.")

        _msg("-> Joining all calculated statistics back to the output polygon layer.")
        elev_case_field = [f.name for f in arcpy.ListFields(elev_stats_tbl)][0]
        arcpy.management.JoinField(out_fc, "ZoneID", elev_stats_tbl, elev_case_field, ["MIN_VALUE", "MAX_VALUE", "MEAN_VALUE", "STD_VALUE", "MEDIAN_VALUE", "COUNT_VALUE"])
        self._rename_field_like(out_fc, "MIN_VALUE", "ELEV_MIN_M"); self._rename_field_like(out_fc, "MAX_VALUE", "ELEV_MAX_M")
        self._rename_field_like(out_fc, "MEAN_VALUE", "ELEV_MEAN_M"); self._rename_field_like(out_fc, "STD_VALUE", "ELEV_STD_M")
        self._rename_field_like(out_fc, "MEDIAN_VALUE", "ELEV_MEDIAN_M"); self._rename_field_like(out_fc, "COUNT_VALUE", "ELEV_SAMPLE_COUNT")

        slope_case_field = [f.name for f in arcpy.ListFields(slope_stats_tbl)][0]
        arcpy.management.JoinField(out_fc, "ZoneID", slope_stats_tbl, slope_case_field, ["MIN_VALUE", "MAX_VALUE", "MEAN_VALUE", "STD_VALUE", "MEDIAN_VALUE", "COUNT_VALUE"])
        self._rename_field_like(out_fc, "MIN_VALUE", "SLOPE_MIN_DEG"); self._rename_field_like(out_fc, "MAX_VALUE", "SLOPE_MAX_DEG")
        self._rename_field_like(out_fc, "MEAN_VALUE", "SLOPE_MEAN_DEG"); self._rename_field_like(out_fc, "STD_VALUE", "SLOPE_STD_DEG")
        self._rename_field_like(out_fc, "MEDIAN_VALUE", "SLOPE_MEDIAN_DEG"); self._rename_field_like(out_fc, "COUNT_VALUE", "SLOPE_SAMPLE_COUNT")

        arcpy.management.JoinField(out_fc, "ZoneID", aspect_stats_tbl, "ZoneID", ["ASPECT_MEAN_DEG", "ASPECT_STD_DEG", "ASPECT_SAMPLE_COUNT"])
        _msg("   - Joins complete. Finalizing fields.")

        _msg("-> Cleaning up temporary slope analysis datasets.")
        try:
            for t in (elev_pts, slope_pts, aspect_pts, elev_pts_z, slope_pts_z, aspect_pts_z, elev_stats_tbl, slope_stats_tbl, aspect_stats_tbl, tin_rast):
                if t and arcpy.Exists(t): arcpy.Delete_management(t)
        except Exception: pass

        _msg(f"--- Slope Analysis for '{label}' COMPLETE. Output: {out_fc} ---")
        return out_fc

    def _calculate_polygon_area(self, fc, area_field, messages):
        _msg(f"-> Utility: Calculating polygon area into field '{area_field}' for '{fc}'")
        if area_field not in [f.name for f in arcpy.ListFields(fc)]:
            arcpy.AddField_management(fc, area_field, "DOUBLE")
        
        try:
            _msg("   - Attempting to calculate geodesic area (most accurate).")
            arcpy.management.CalculateGeometryAttributes(fc, [[area_field, "AREA_GEODESIC"]], area_unit="Square Meters")
            _msg(f"   - SUCCESS: Geodesic area populated in '{area_field}'.")
            return
        except Exception as e:
            _warn(f"   - Geodesic calculation failed: {e}. Trying planar method.")
        
        try:
            _msg("   - Attempting to calculate planar area.")
            arcpy.management.CalculateGeometryAttributes(fc, [[area_field, "AREA"]], area_unit="Square Meters")
            _msg(f"   - SUCCESS: Planar area populated in '{area_field}'.")
            return
        except Exception as e:
            raise arcpy.ExecuteError(f"CRITICAL: Polygon area calculation failed for both geodesic and planar methods. Error: {e}")

    def _rename_field_like(self, fc, orig_prefix, newname):
        fields = [f.name for f in arcpy.ListFields(fc)]
        for fname in fields:
            if fname.lower().startswith(orig_prefix.lower()):
                if newname in fields:
                    _warn(f"   - Field '{newname}' already exists. Cannot rename '{fname}'.")
                    return False
                try:
                    arcpy.management.AlterField(fc, fname, new_field_name=newname, new_field_alias=newname)
                    _msg(f"   - Renamed field '{fname}' to '{newname}'.")
                    return True
                except Exception as e:
                    _warn(f"   - Failed to rename field '{fname}' to '{newname}': {e}")
                    continue
        return False
