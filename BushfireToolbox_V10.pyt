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
    _msg(f"Scouring geodatabase {gdb_workspace} for anything named '{name}'...")
    prev = arcpy.env.workspace
    try:
        arcpy.env.workspace = gdb_workspace

        root_candidate = os.path.join(gdb_workspace, name)
        try:
            if arcpy.Exists(root_candidate):
                _msg(f"  • Deleting root feature class {root_candidate}.")
                arcpy.management.Delete(root_candidate)
        except Exception as ex_root:
            _warn(f"  • Could not delete root candidate {root_candidate}: {ex_root}")

        try:
            for fc in arcpy.ListFeatureClasses(name):
                _msg(f"  • Executing feature class {fc} in root.")
                try:
                    arcpy.management.Delete(fc)
                except Exception as ex_fc:
                    _warn(f"    • Could not delete {fc} from root: {ex_fc}")
        except Exception as ex_list_root:
            _warn(f"  • ListFeatureClasses failed in root: {ex_list_root}")

        try:
            for ras in arcpy.ListRasters(name):
                _msg(f"  • Banishing raster {ras}.")
                try:
                    arcpy.management.Delete(ras)
                except Exception as ex_r:
                    _warn(f"    • Could not delete raster {ras}: {ex_r}")
        except Exception as ex_list_rast:
            _warn(f"  • ListRasters failed in root: {ex_list_rast}")

        try:
            for ds in arcpy.ListDatasets(feature_type='feature') or []:
                ds_path = os.path.join(gdb_workspace, ds)
                candidate_in_ds = os.path.join(ds_path, name)
                try:
                    if arcpy.Exists(candidate_in_ds):
                        _msg(f"  • Deleting {candidate_in_ds} inside dataset {ds}.")
                        arcpy.management.Delete(candidate_in_ds)
                except Exception as ex_cds:
                    _warn(f"    • Could not delete {candidate_in_ds}: {ex_cds}")

                try:
                    arcpy.env.workspace = ds_path
                    for fc in arcpy.ListFeatureClasses(name):
                        _msg(f"  • Executing feature class {fc} inside {ds}.")
                        try:
                            arcpy.management.Delete(fc)
                        except Exception as ex_fc_ds:
                            _warn(f"    • Could not delete {fc} from {ds}: {ex_fc_ds}")
                    for ras in arcpy.ListRasters(name):
                        _msg(f"  • Banishing raster {ras} from {ds}.")
                        try:
                            arcpy.management.Delete(ras)
                        except Exception as ex_rds:
                            _warn(f"    • Could not delete raster {ras} from {ds}: {ex_rds}")
                except Exception as ex_inner_ds:
                    _warn(f"  • Could not enumerate inside dataset {ds}: {ex_inner_ds}")
        except Exception as ex_ds_list:
            _warn(f"  • Could not list datasets: {ex_ds_list}")

    finally:
        arcpy.env.workspace = prev
    _msg("Global purge complete.")

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
    _msg(f"Object '{base}' exists; renaming to '{candidate}'.")
    arcpy.management.Rename(path, candidate, data_type)
    return candidate_path

def _prepare_output(path, overwrite, data_type="FeatureClass", gdb_workspace=None):
    name = os.path.basename(path)
    if overwrite:
        _msg(f"Overwrite enabled. Removing previous '{name}'...")
        if gdb_workspace:
            _delete_name_globally(gdb_workspace, name)
        elif arcpy.Exists(path):
            arcpy.management.Delete(path)
        return path
    else:
        _msg(f"Overwrite disabled. Avoiding name clash for '{name}'.")
        return _unique_rename(path, data_type)

def _tin_output_path(workspace, tin_name):
    if workspace.lower().endswith(".gdb"):
        base_folder = os.path.dirname(workspace)
        tin_folder = os.path.join(base_folder, "TINs")
    else:
        tin_folder = os.path.join(workspace, "TINs")
    if not os.path.exists(tin_folder):
        _msg(f"Constructing TIN lair at '{tin_folder}'.")
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
            "Creates site and building buffers, clips contours, SVTM and BFPL, builds a TIN, "
            "runs Above/Below splitting, and runs integrated slope analysis on SVTM polygons."
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
                _msg("Consulting the remote oracle for project numbers (feature service)...")
                with arcpy.da.SearchCursor(FEATURE_SERVICE_URL, ["project_number"]) as cursor:
                    vals = sorted({row[0] for row in cursor if row[0]})
                parameters[1].filter.list = vals
                _msg(f"Loaded {len(vals)} project numbers.")
            except Exception as ex:
                _warn(f"Could not load project numbers: {ex}")
        return

    def execute(self, parameters, messages):
        _msg("Welcome to Bushfire Preliminary Assessment V10.")

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
        _msg(f"Building: {building_fc}")
        _msg(f"Building buffer: {building_buffer_distance} m")
        _msg(f"Above/Below split elevation: {split_elev} m")
        _msg(f"Overwrite: {overwrite_outputs}")
        _msg(f"Add to map: {add_to_map}")

        try:
            bdesc = arcpy.Describe(building_fc)
            if getattr(bdesc, "shapeType", "").lower() != "polygon":
                raise arcpy.ExecuteError(f"Building feature class must be polygon. Found: {bdesc.shapeType}.")
        except Exception as ex:
            raise arcpy.ExecuteError(f"Could not validate building feature class geometry: {ex}")

        fds_path, sr = _ensure_fds(workspace)

        safe_project = project_number.replace("'", "''")
        where = f"project_number = '{safe_project}'"
        subject_layer = "subject_site_layer"
        _msg(f"Selecting subject site with: {where}")
        arcpy.management.MakeFeatureLayer(FEATURE_SERVICE_URL, subject_layer, where)
        if int(arcpy.management.GetCount(subject_layer).getOutput(0)) == 0:
            raise arcpy.ExecuteError(f"No features found for project_number {project_number}.")

        buffer_name = f"AEP{project_number}_Site_Buffer_{int(buffer_distance)}"
        buffer_path = os.path.join(fds_path, buffer_name)
        buffer_path = _prepare_output(buffer_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Buffer(subject_layer, buffer_path, f"{buffer_distance} Meters", dissolve_option="ALL")
        _msg(f"Site buffer: {buffer_path}")

        clipped_name = f"AEP{project_number}_2m_Contours"
        clipped_path = os.path.join(fds_path, clipped_name)
        clipped_path = _prepare_output(clipped_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Clip(contours_fc, buffer_path, clipped_path)
        _msg(f"Contours clipped: {clipped_path}")

        svtm_date = datetime.now().strftime("%Y%m%d")
        svtm_name = f"AEP{project_number}_SVTM_{svtm_date}"
        svtm_path = os.path.join(fds_path, svtm_name)
        svtm_path = _prepare_output(svtm_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.management.MakeFeatureLayer(SVTM_URL, "svtm_layer")
        arcpy.analysis.Clip("svtm_layer", buffer_path, svtm_path)
        _msg(f"SVTM clipped: {svtm_path}")

        bfpl_path = None
        try:
            bfpl_name = f"AEP{project_number}_BFPL_{svtm_date}"
            bfpl_path = os.path.join(fds_path, bfpl_name)
            bfpl_path = _prepare_output(bfpl_path, overwrite_outputs, "FeatureClass", workspace)
            arcpy.management.MakeFeatureLayer(BFPL_URL, "bfpl_layer")
            arcpy.analysis.Clip("bfpl_layer", buffer_path, bfpl_path)
            _msg(f"BFPL clipped: {bfpl_path}")
        except Exception as ex_bfpl:
            _warn(f"Could not clip BFPL: {ex_bfpl}")

        bbuf_name = f"AEP{project_number}_Building_Buffer_{int(building_buffer_distance)}M"
        bbuf_path = os.path.join(fds_path, bbuf_name)
        bbuf_path = _prepare_output(bbuf_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Buffer(building_fc, bbuf_path, f"{building_buffer_distance} Meters", dissolve_option="ALL")
        _msg(f"Building buffer: {bbuf_path}")

        svtm_bbuf_name = f"AEP{project_number}_SVTM_Bld_Buffer_{svtm_date}"
        svtm_bbuf_path = os.path.join(fds_path, svtm_bbuf_name)
        svtm_bbuf_path = _prepare_output(svtm_bbuf_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Clip(svtm_path, bbuf_path, svtm_bbuf_path)
        _msg(f"SVTM within building buffer: {svtm_bbuf_path}")

        svtm_bbuf_erase_name = f"AEP{project_number}_SVTM_Bld_Buffer_NoBld_{svtm_date}"
        svtm_bbuf_erase_path = os.path.join(fds_path, svtm_bbuf_erase_name)
        svtm_bbuf_erase_path = _prepare_output(svtm_bbuf_erase_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Erase(svtm_bbuf_path, building_fc, svtm_bbuf_erase_path)
        _msg(f"SVTM bld buffer no-buildings: {svtm_bbuf_erase_path}")

        tin_name = f"AEP{project_number}_TIN"
        tin_path = _tin_output_path(workspace, tin_name)
        if arcpy.Exists(tin_path):
            _msg(f"Sacking existing TIN: {tin_path}")
            arcpy.management.Delete(tin_path)
        z_field = self._infer_z_field(clipped_path)
        _msg(f"Elevation field for TIN: {z_field}")
        in_feats = [[clipped_path, z_field, "hardline"]]
        _msg(f"Creating TIN at {tin_path} ...")
        arcpy.ddd.CreateTin(out_tin=tin_path, spatial_reference=sr, in_features=in_feats, constrained_delaunay="DELAUNAY")
        _msg("TIN created.")

        _msg("Creating DSM (1 m) from TIN and performing Above/Below splitting...")
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
        _msg(f"DSM saved to {dsm_path}.")

        _msg("Raster-to-polygon and elevation field population...")
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

        _msg(f"Selecting polygons Above/Below {split_elev} m and dissolving...")
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
            _warn("No polygons found above the threshold.")

        if int(arcpy.management.GetCount(lesseq_tmp).getOutput(0)) > 0:
            arcpy.management.Dissolve(lesseq_tmp, lesseq_diss)
            arcpy.AddField_management(lesseq_diss, "Relation", "TEXT", field_length=10)
            with arcpy.da.UpdateCursor(lesseq_diss, ["Relation"]) as cur:
                for row in cur:
                    row[0] = "LessEqual"
                    cur.updateRow(row)
            parts.append(lesseq_diss)
        else:
            _warn("No polygons found below or equal to the threshold.")

        final_name = f"AEP{project_number}_DSM_AboveBelow_{svtm_date}"
        final_path = os.path.join(fds_path, final_name)
        final_path = _prepare_output(final_path, overwrite_outputs, "FeatureClass", workspace)
        if parts:
            arcpy.management.Merge(parts, final_path)
            _msg(f"Above/Below polygons created: {final_path}")
        else:
            _warn("No Above/Below polygons to merge.")
        
        # Update Relation field values and rename the dataset
        if final_path and arcpy.Exists(final_path):
            try:
                _msg("Updating Relation field values to friendly names...")
                fields_list = [f.name for f in arcpy.ListFields(final_path)]
                if "Relation" in fields_list:
                    with arcpy.da.UpdateCursor(final_path, ["Relation"]) as ucur:
                        for urow in ucur:
                            rel_val = (urow[0] or "").strip()
                            if rel_val.lower() == "lessequal":
                                urow[0] = "Up Slope"
                            elif rel_val.lower() == "greater":
                                urow[0] = "Down Slope"
                            ucur.updateRow(urow)
                    _msg("Relation field values updated.")
                
                # Rename the dataset to the new format
                new_name = f"AEP{project_number}_Slope_Class_{int(split_elev)}m"
                new_path = os.path.join(fds_path, new_name)
                
                # Handle existing dataset with same name
                if arcpy.Exists(new_path):
                    if overwrite_outputs:
                        _msg(f"Overwrite enabled. Removing previous '{new_name}'...")
                        arcpy.management.Delete(new_path)
                    else:
                        new_path = _unique_rename(new_path, "FeatureClass")
                        new_name = os.path.basename(new_path)
                
                _msg(f"Renaming {final_name} to {new_name}...")
                arcpy.management.Rename(final_path, new_path, "FeatureClass")
                final_path = new_path
                _msg(f"Renamed to: {final_path}")
            except Exception as ex_rename:
                _warn(f"Could not update/rename Above/Below layer: {ex_rename}")

        overlay_ok = True
        try:
            if not final_path or not arcpy.Exists(final_path):
                _warn("Above/Below polygon layer not found; skipping Identity overlay to SVTM layers.")
                overlay_ok = False
            else:
                svtm_variants = [
                    ("SVTM (site)", svtm_path),
                    ("SVTM Building Buffer No Building", svtm_bbuf_erase_path)
                ]
                for label, svtm_fc in svtm_variants:
                    if not svtm_fc or not arcpy.Exists(svtm_fc):
                        _warn(f"Skipping Identity for {label}: source does not exist: {svtm_fc}")
                        overlay_ok = False
                        continue

                    _msg(f"Identity overlay for {label}...")
                    temp_svtm = os.path.join("in_memory", f"svtm_temp_{datetime.now().strftime('%H%M%S')}")
                    ident_tmp = os.path.join("in_memory", f"svtm_ident_{datetime.now().strftime('%H%M%S')}")

                    arcpy.management.CopyFeatures(svtm_fc, temp_svtm)

                    existing_fields = [f.name for f in arcpy.ListFields(temp_svtm)]
                    if "Relation" in existing_fields:
                        try:
                            arcpy.management.DeleteField(temp_svtm, "Relation")
                        except Exception as ex_del:
                            _warn(f"Could not delete existing 'Relation' on temp SVTM for {label}: {ex_del}")

                    arcpy.analysis.Identity(temp_svtm, final_path, ident_tmp)

                    try:
                        if arcpy.Exists(svtm_fc):
                            arcpy.management.Delete(svtm_fc)
                        arcpy.management.CopyFeatures(ident_tmp, svtm_fc)

                        fields_after = [f.name for f in arcpy.ListFields(svtm_fc)]
                        if "Relation" in fields_after:
                            if "Slope_Type" not in fields_after:
                                arcpy.AddField_management(svtm_fc, "Slope_Type", "TEXT", field_length=30)
                            with arcpy.da.UpdateCursor(svtm_fc, ["Relation", "Slope_Type"]) as ucur:
                                for urow in ucur:
                                    rel = (urow[0] or "").strip()
                                    if rel == "LessEqual":
                                        mapped = "Down Slope"
                                    elif rel == "Greater":
                                        mapped = "Up Slope"
                                    else:
                                        mapped = rel if rel else None
                                    urow[1] = mapped
                                    ucur.updateRow(urow)
                            try:
                                arcpy.management.DeleteField(svtm_fc, "Relation")
                            except Exception as ex_del_rel:
                                _warn(f"Could not delete 'Relation' on {label}: {ex_del_rel}")
                        _msg(f"Identity overlay applied for {label}.")
                    except Exception as ex_copy:
                        overlay_ok = False
                        _warn(f"Could not overwrite {label} with identity result: {ex_copy}")
        except Exception as ex_ident:
            overlay_ok = False
            _warn(f"Failed while overlaying Above/Below onto SVTM: {ex_ident}")

        arcpy.env.cellSize = old_cell
        arcpy.env.mask = None
        arcpy.env.extent = None
        arcpy.env.outputCoordinateSystem = None

        if overlay_ok and final_path and arcpy.Exists(final_path):
            try:
                self._run_slope_analysis(in_tin=tin_path, in_polygons=svtm_path, add_to_map=add_to_map, label="SVTM (site)")
            except Exception as ex:
                _warn(f"Slope Analysis failed for SVTM (site): {ex}")
            try:
                self._run_slope_analysis(in_tin=tin_path, in_polygons=svtm_bbuf_erase_path, add_to_map=add_to_map, label="SVTM Building Buffer No Building")
            except Exception as ex:
                _warn(f"Slope Analysis failed for SVTM Building Buffer No Building: {ex}")
        else:
            _warn("Skipping slope analysis because Above/Below overlay did not complete or layer is missing.")

        try:
            arcpy.management.Delete("in_memory")
        except Exception:
            pass

        if add_to_map:
            outputs_to_add = [
                svtm_bbuf_erase_path,
                svtm_path,
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
            _msg("Not adding outputs to the map by request.")

        _msg("Bushfire Preliminary Assessment V10 completed.")
        return

    def _infer_z_field(self, fc):
        _msg(f"Attempting to divine elevation field in {fc}...")
        fields = [f for f in arcpy.ListFields(fc) if f.type in ("Integer", "SmallInteger", "Double", "Single")]
        candidates = ("ELEVATION", "ELEV", "Z", "CONTOUR", "VALUE")
        for cand in candidates:
            for f in fields:
                if f.name.upper() == cand:
                    _msg(f"Using field '{f.name}'.")
                    return f.name
        if fields:
            _warn(f"No standard elevation field; defaulting to '{fields[0].name}'.")
            return fields[0].name
        raise arcpy.ExecuteError("No numeric elevation field found for contours.")

    def _add_outputs_to_map(self, paths):
        try:
            aprx = arcpy.mp.ArcGISProject("CURRENT")
            m = aprx.activeMap
            if not m:
                _warn("No active map detected.")
                return
            for lyr_path in paths:
                try:
                    if not lyr_path or not arcpy.Exists(lyr_path):
                        continue
                    m.addDataFromPath(lyr_path)
                    _msg(f"Layer added to map: {lyr_path}")
                except Exception as ex_inner:
                    _warn(f"Could not add {lyr_path} to map: {ex_inner}")
            _msg("Map additions complete.")
        except Exception as ex:
            _warn(f"Could not add outputs to map: {ex}")

    def _run_slope_analysis(self, in_tin, in_polygons, add_to_map, label=""):
        arcpy.env.overwriteOutput = True
        if not (in_tin and arcpy.Exists(in_tin)):
            raise arcpy.ExecuteError("Slope Analysis: Input TIN does not exist.")
        if not (in_polygons and arcpy.Exists(in_polygons)):
            raise arcpy.ExecuteError("Slope Analysis: Input polygons do not exist.")

        _msg(f"Slope Analysis starting for '{label}'")

        try:
            desc = arcpy.Describe(in_tin)
            ds_type = getattr(desc, "datasetType", None) or getattr(desc, "dataType", None) or ""
            if ds_type is None or "tin" not in str(ds_type).lower():
                raise arcpy.ExecuteError(f"Slope Analysis: Input is not a TIN (datasetType/dataType='{ds_type}').")
        except arcpy.ExecuteError:
            raise
        except Exception as e:
            raise arcpy.ExecuteError(f"Slope Analysis: Failed to validate TIN: {e}")

        cell_size = 1
        target_sr = arcpy.SpatialReference(8058)
        aprx = None
        try:
            aprx = arcpy.mp.ArcGISProject("CURRENT")
            default_gdb = aprx.defaultGeodatabase
        except Exception:
            default_gdb = arcpy.env.workspace
            if not default_gdb:
                raise arcpy.ExecuteError("Slope Analysis: Unable to determine default geodatabase.")

        _msg(f"Slope Analysis: default GDB = {default_gdb}")

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

        _msg("TIN -> 1m raster...")
        tin_rast = os.path.join(mem, f"tin_rast_{guid}")
        try:
            arcpy.ddd.TinRaster(in_tin, tin_rast, "FLOAT", "#", "CELLSIZE", str(cell_size))
        except Exception:
            tin_rast = os.path.join(default_gdb, f"tin_rast_{guid}")
            arcpy.ddd.TinRaster(in_tin, tin_rast, "FLOAT", "#", "CELLSIZE", str(cell_size))

        _msg("Slope (degrees)...")
        slope_rast = arcpy.sa.Slope(tin_rast, "DEGREE", z_factor=1)
        _msg("Aspect (degrees)...")
        aspect_rast = arcpy.sa.Aspect(tin_rast)

        _msg("Preparing polygon output and ZoneID...")
        if arcpy.Exists(out_fc):
            _msg(f"Overwriting {out_fc}")
            arcpy.Delete_management(out_fc)
        arcpy.management.CopyFeatures(in_polygons, out_fc)
        zone_field = "ZoneID"
        if zone_field not in [f.name for f in arcpy.ListFields(out_fc)]:
            arcpy.AddField_management(out_fc, zone_field, "LONG")
        oid_field = arcpy.Describe(out_fc).OIDFieldName
        arcpy.management.CalculateField(out_fc, zone_field, f"!{oid_field}!", "PYTHON3")

        _msg("RasterToPoint sample generation...")
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

        _msg("Computing statistics...")
        elev_stats_tbl = os.path.join(mem, f"elev_stats_{guid}")
        slope_stats_tbl = os.path.join(mem, f"slope_stats_{guid}")
        stat_fields = [["VALUE", "MIN"], ["VALUE", "MAX"], ["VALUE", "MEAN"], ["VALUE", "STD"], ["VALUE", "MEDIAN"], ["VALUE", "COUNT"]]
        arcpy.analysis.Statistics(elev_pts_z, elev_stats_tbl, stat_fields, case_field=zone_field_elev)
        arcpy.analysis.Statistics(slope_pts_z, slope_stats_tbl, stat_fields, case_field=zone_field_slope)

        _msg("Circular statistics for aspect...")
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

        _msg("Slope class percentages (Tabulate Area)...")
        class_definitions = {1: "0_5", 2: "5_15", 3: "15_30", 4: "30_45", 5: "45_plus"}
        remap = arcpy.sa.RemapRange([[0, 5, 1], [5, 15, 2], [15, 30, 3], [30, 45, 4], [45, 360, 5]])
        slope_class_rast = arcpy.sa.Reclassify(slope_rast, "VALUE", remap, "NODATA")
        class_rast_temp = os.path.join(mem, f"slope_class_{guid}")
        slope_class_rast.save(class_rast_temp)
        tab_area_tbl = os.path.join(mem, f"tab_area_{guid}")
        arcpy.sa.TabulateArea(out_fc, "ZoneID", class_rast_temp, "VALUE", tab_area_tbl, cell_size)

        _msg("Joining statistics into polygons...")
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

        tab_case_field = [f.name for f in arcpy.ListFields(tab_area_tbl)][0]
        arcpy.management.JoinField(out_fc, "ZoneID", tab_area_tbl, tab_case_field, None)

        tab_fields_tbl = [f for f in arcpy.ListFields(tab_area_tbl) if f.name != tab_case_field and f.type in ("Double", "Single", "Integer", "SmallInteger", "OID")]
        class_field_mappings = []
        for f in tab_fields_tbl:
            fname = f.name
            m = re.search(r'(\d+)', fname)
            code = int(m.group(1)) if m else None
            label = class_definitions.get(code, str(code))
            area_field_name = f"SLOPE_CLASS_AREA_{label}_SQM"
            pct_field_name = f"SLOPE_PCT_{label}"
            if len(area_field_name) > 64:
                area_field_name = area_field_name[:64]
            if len(pct_field_name) > 64:
                pct_field_name = pct_field_name[:64]
            class_field_mappings.append((fname, area_field_name, pct_field_name))

        area_field = "POLY_AREA_SQM"
        try:
            self._calculate_polygon_area(out_fc, area_field, arcpy)
        except Exception as e:
            tb = traceback.format_exc()
            raise arcpy.ExecuteError(f"Failed to calculate polygon areas: {e}\n{tb}")

        for orig_field, area_field_name, pct_field_name in class_field_mappings:
            if area_field_name not in [f.name for f in arcpy.ListFields(out_fc)]:
                arcpy.AddField_management(out_fc, area_field_name, "DOUBLE")
            arcpy.management.CalculateField(
                out_fc,
                area_field_name,
                expression=f"!{orig_field}! if !{orig_field}! is not None else 0",
                expression_type="PYTHON3"
            )
            if pct_field_name not in [f.name for f in arcpy.ListFields(out_fc)]:
                arcpy.AddField_management(out_fc, pct_field_name, "DOUBLE")
            arcpy.management.CalculateField(
                out_fc,
                pct_field_name,
                expression=(
                    f"("
                    f"( !{area_field_name}! if !{area_field_name}! is not None else 0 ) / "
                    f"( !{area_field}! if !{area_field}! is not None and !{area_field}! > 0 else 1 )"
                    f") * 100"
                ),
                expression_type="PYTHON3"
            )

        _msg(f"Slope analysis output created: {out_fc}")

        if add_to_map:
            try:
                aprx = arcpy.mp.ArcGISProject("CURRENT")
                m = aprx.activeMap or (aprx.listMaps()[0] if aprx.listMaps() else None)
                if m:
                    m.addDataFromPath(out_fc)
                    _msg("Slope analysis output added to map.")
                else:
                    _warn("No open map to add slope output.")
            except Exception as e:
                _warn(f"Failed to add slope output to map: {e}")

        try:
            for t in (elev_pts, slope_pts, aspect_pts, elev_pts_z, slope_pts_z, aspect_pts_z,
                      elev_stats_tbl, slope_stats_tbl, aspect_stats_tbl, class_rast_temp, tab_area_tbl, tin_rast):
                if t and arcpy.Exists(t):
                    arcpy.Delete_management(t)
        except Exception:
            pass

        _msg(f"Slope Analysis finished for '{label}'.")

    def _calculate_polygon_area(self, fc, area_field, messages):
        if area_field not in [f.name for f in arcpy.ListFields(fc)]:
            arcpy.AddField_management(fc, area_field, "DOUBLE")
        tried = []
        try:
            _msg("Calculating AREA_GEODESIC (Square Meters)...")
            arcpy.management.CalculateGeometryAttributes(fc, [[area_field, "AREA_GEODESIC"]], area_unit="Square Meters")
            _msg(f"Geodesic area calculated into '{area_field}'.")
            return
        except Exception as e:
            tried.append(("AREA_GEODESIC", "Square Meters", str(e)))
        try:
            _msg("Falling back to planar AREA (Square Meters)...")
            arcpy.management.CalculateGeometryAttributes(fc, [[area_field, "AREA"]], area_unit="Square Meters")
            _msg(f"Planar area calculated into '{area_field}'.")
            return
        except Exception as e:
            tried.append(("AREA", "Square Meters", str(e)))
        alt_units = ["Square Meters", "SquareMeters", "SQUARE_METERS", "SQUAREMETERS", "Square Meters"]
        for alt in alt_units:
            try:
                _msg(f"Attempting AREA with unit '{alt}'...")
                arcpy.management.CalculateGeometryAttributes(fc, [[area_field, "AREA"]], area_unit=alt)
                _msg(f"Area calculated into '{area_field}' using unit '{alt}'.")
                return
            except Exception as e:
                tried.append(("AREA", alt, str(e)))
                continue
        msg_lines = ["Failed to calculate polygon areas; attempted:"]
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
