# -*- coding: utf-8 -*-
import arcpy
import os
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
    """
    Attempt to delete any object named `name` anywhere in the file geodatabase `gdb_workspace`.
    This function is defensive: it checks the root, each feature dataset and tries both
    listing and explicit path deletes to handle cases where name collisions are reported by ArcGIS.
    """
    _msg(f"Scouring geodatabase {gdb_workspace} for anything named '{name}' (like a cartographic inquisition)...")
    prev = arcpy.env.workspace
    try:
        arcpy.env.workspace = gdb_workspace

        # Explicitly check the root path
        root_candidate = os.path.join(gdb_workspace, name)
        try:
            if arcpy.Exists(root_candidate):
                _msg(f"  • Deleting root feature class {root_candidate}.")
                arcpy.management.Delete(root_candidate)
        except Exception as ex_root:
            _warn(f"  • Could not delete root candidate {root_candidate}: {ex_root}")

        # Use listing in the root workspace
        try:
            for fc in arcpy.ListFeatureClasses(name):
                _msg(f"  • Executing feature class {fc} in root. It's a fair cop.")
                try:
                    arcpy.management.Delete(fc)
                except Exception as ex_fc:
                    _warn(f"    • Could not delete {fc} from root: {ex_fc}")
        except Exception as ex_list_root:
            _warn(f"  • ListFeatureClasses failed in root: {ex_list_root}")

        try:
            for ras in arcpy.ListRasters(name):
                _msg(f"  • Banishing raster {ras} to the abyss.")
                try:
                    arcpy.management.Delete(ras)
                except Exception as ex_r:
                    _warn(f"    • Could not delete raster {ras}: {ex_r}")
        except Exception as ex_list_rast:
            _warn(f"  • ListRasters failed in root: {ex_list_rast}")

        # Iterate feature datasets and look for named items inside them
        try:
            for ds in arcpy.ListDatasets(feature_type='feature') or []:
                ds_path = os.path.join(gdb_workspace, ds)
                # Explicit path candidate inside dataset
                candidate_in_ds = os.path.join(ds_path, name)
                try:
                    if arcpy.Exists(candidate_in_ds):
                        _msg(f"  • Deleting feature class {candidate_in_ds} inside dataset {ds}.")
                        arcpy.management.Delete(candidate_in_ds)
                except Exception as ex_cds:
                    _warn(f"    • Could not delete {candidate_in_ds}: {ex_cds}")

                # Also set workspace to ds_path and list (defensive)
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
    _msg("Global purge complete. Bring out the next dataset!")

def _unique_rename(path, data_type="FeatureClass"):
    """
    If `path` exists, rename it by appending a date (and a counter if necessary).
    Return the new path (not the original). This is important so callers get the actual new name.
    """
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
    return candidate_path

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

class Toolbox(object):
    def __init__(self):
        self.label = "Buffer Toolbox V9"
        self.alias = "BufferToolboxV9"
        self.tools = [SiteBufferToolV9]

class SiteBufferToolV9(object):
    def __init__(self):
        self.label = "Bushfire Preliminary Assessment"
        self.description = (
            "Buffers the project, clips contours and SVTM, buffers the building, "
            "erases the building, and clips BFPL to site buffer. "
            "Now tidied to only produce the layers you asked for."
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
            p_building, p_build_buffer,
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
        _msg("Welcome to the Bushfire Preliminary Assessment. Streamlined outputs engaged.")

        workspace = parameters[0].valueAsText
        project_number = parameters[1].valueAsText
        buffer_distance = float(parameters[2].value)
        contours_fc = parameters[3].valueAsText
        building_fc = parameters[4].valueAsText
        building_buffer_distance = float(parameters[5].value)
        overwrite_outputs = bool(parameters[6].value)
        add_to_map = bool(parameters[7].value)

        _msg(f"Workspace has been declared: {workspace}")
        _msg(f"Project of interest: {project_number}")
        _msg(f"Site buffer distance: {buffer_distance} m (a comfortable stroll).")
        _msg(f"Contours provided: {contours_fc}")
        _msg(f"Building outline: {building_fc}")
        _msg(f"Building buffer distance: {building_buffer_distance} m (for health and safety reasons).")
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

        # Site buffer (KEEP)
        _msg("Applying site buffer. Stand well back; it may go off.")
        buffer_name = f"AEP{project_number}_Site_Buffer_{int(buffer_distance)}"
        buffer_path = os.path.join(fds_path, buffer_name)
        buffer_path = _prepare_output(buffer_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Buffer(subject_layer, buffer_path, f"{buffer_distance} Meters", dissolve_option="ALL")
        _msg(f"Site buffer created at {buffer_path}.")

        # Contours clip (KEEP)
        _msg("Clipping contours to the site buffer.")
        clipped_name = f"AEP{project_number}_2m_Contours"
        clipped_path = os.path.join(fds_path, clipped_name)
        clipped_path = _prepare_output(clipped_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Clip(contours_fc, buffer_path, clipped_path)
        _msg(f"Contours clipped into {clipped_path}.")

        # SVTM clip to site buffer (KEEP)
        svtm_date = datetime.now().strftime("%Y%m%d")
        svtm_name = f"AEP{project_number}_SVTM_{svtm_date}"
        svtm_path = os.path.join(fds_path, svtm_name)
        svtm_path = _prepare_output(svtm_path, overwrite_outputs, "FeatureClass", workspace)
        _msg("Summoning SVTM layer from distant lands and clipping to site buffer...")
        arcpy.management.MakeFeatureLayer(SVTM_URL, "svtm_layer")
        arcpy.analysis.Clip("svtm_layer", buffer_path, svtm_path)
        _msg(f"SVTM clipped to {svtm_path}.")

        # BFPL clip to site buffer (KEEP)
        bfpl_path = None
        try:
            bfpl_name = f"AEP{project_number}_BFPL_{svtm_date}"
            bfpl_path = os.path.join(fds_path, bfpl_name)
            bfpl_path = _prepare_output(bfpl_path, overwrite_outputs, "FeatureClass", workspace)
            _msg("Summoning BFPL layer and clipping to site buffer...")
            arcpy.management.MakeFeatureLayer(BFPL_URL, "bfpl_layer")
            arcpy.analysis.Clip("bfpl_layer", buffer_path, bfpl_path)
            _msg(f"BFPL clipped to {bfpl_path}.")
        except Exception as ex_bfpl:
            _warn(f"Could not clip BFPL layer: {ex_bfpl}")

        # Building buffer (KEEP)
        _msg("Buffering building outline.")
        bbuf_name = f"AEP{project_number}_Building_Buffer_{int(building_buffer_distance)}M"
        bbuf_path = os.path.join(fds_path, bbuf_name)
        bbuf_path = _prepare_output(bbuf_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Buffer(building_fc, bbuf_path, f"{building_buffer_distance} Meters", dissolve_option="ALL")
        _msg(f"Building buffer created at {bbuf_path}.")

        # SVTM clip to building buffer (needed for next step)
        _msg("Clipping SVTM to the building buffer.")
        svtm_bbuf_name = f"AEP{project_number}_SVTM_Bld_Buffer_{svtm_date}"
        svtm_bbuf_path = os.path.join(fds_path, svtm_bbuf_name)
        svtm_bbuf_path = _prepare_output(svtm_bbuf_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Clip(svtm_path, bbuf_path, svtm_bbuf_path)
        _msg(f"SVTM within building buffer saved to {svtm_bbuf_path}.")

        # Erase buildings from SVTM building buffer (KEEP)
        _msg("Erasing the building footprint from the SVTM building buffer.")
        svtm_bbuf_erase_name = f"AEP{project_number}_SVTM_Bld_Buffer_NoBld_{svtm_date}"
        svtm_bbuf_erase_path = os.path.join(fds_path, svtm_bbuf_erase_name)
        svtm_bbuf_erase_path = _prepare_output(svtm_bbuf_erase_path, overwrite_outputs, "FeatureClass", workspace)
        arcpy.analysis.Erase(svtm_bbuf_path, building_fc, svtm_bbuf_erase_path)
        _msg(f"Building erased from SVTM buffer; results at {svtm_bbuf_erase_path}.")

        # Cleanup of any in_memory workspace just in case
        try:
            arcpy.management.Delete("in_memory")
        except Exception:
            pass

        # Add only the requested outputs to the map
        if add_to_map:
            _msg("Adding requested outputs to the current map.")
            outputs_to_add = [
                svtm_bbuf_erase_path,  # svtm_bld_buffer_nobld...
                svtm_path,             # svtm...
                bbuf_path,             # building buffer...
                buffer_path,           # site_buffer...
                clipped_path           # 2m_contours
            ]
            # include BFPL clipped layer if it was created
            if bfpl_path and arcpy.Exists(bfpl_path):
                outputs_to_add.append(bfpl_path)  # bfpl...
            self._add_outputs_to_map(outputs_to_add)
        else:
            _msg("Not adding outputs to the map by request.")

        _msg("Bushfire Preliminary Assessment completed (tidy edition).")
        return

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
