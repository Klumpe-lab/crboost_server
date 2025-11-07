# services/mdoc_service.py
"""
Service for parsing, writing, and extracting data from .mdoc files.
"""

import glob
from pathlib import Path
from typing import Dict, Any
from functools import lru_cache


class MdocService:
    """Singleton service for all .mdoc file interactions."""

    def get_autodetect_params(self, mdocs_glob: str) -> Dict[str, Any]:
        """
        Parse the first mdoc file found by the glob and extract key parameters
        for state auto-detection.
        This logic is from the original app_state.py.
        """
        mdoc_files = glob.glob(mdocs_glob)
        if not mdoc_files:
            return {}

        mdoc_path = Path(mdoc_files[0])
        result = {}
        header_data = {}
        first_section = {}
        in_zvalue_section = False

        try:
            with open(mdoc_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    if line.startswith("[ZValue"):
                        in_zvalue_section = True
                    elif in_zvalue_section and "=" in line:
                        key, value = [x.strip() for x in line.split("=", 1)]
                        first_section[key] = value
                    elif not in_zvalue_section and "=" in line:
                        key, value = [x.strip() for x in line.split("=", 1)]
                        header_data[key] = value

            # Extract values, preferring header, falling back to first section
            if "PixelSpacing" in header_data:
                result["pixel_spacing"] = float(header_data["PixelSpacing"])
            elif "PixelSpacing" in first_section:
                result["pixel_spacing"] = float(first_section["PixelSpacing"])

            if "Voltage" in header_data:
                result["voltage"] = float(header_data["Voltage"])
            elif "Voltage" in first_section:
                result["voltage"] = float(first_section["Voltage"])

            if "ImageSize" in header_data:
                result["image_size"] = header_data["ImageSize"].replace(" ", "x")
            elif "ImageSize" in first_section:
                result["image_size"] = first_section["ImageSize"].replace(" ", "x")

            if "ExposureDose" in first_section:
                result["exposure_dose"] = float(first_section["ExposureDose"])
            elif "ExposureDose" in header_data:
                result["exposure_dose"] = float(header_data["ExposureDose"])

            if "TiltAxisAngle" in first_section:
                result["tilt_axis_angle"] = float(first_section["TiltAxisAngle"])
            elif "Tilt axis angle" in header_data:
                result["tilt_axis_angle"] = float(header_data["Tilt axis angle"])

            return result

        except Exception as e:
            print(f"[ERROR] MdocService failed to parse {mdoc_path}: {e}")
            return {}

    def parse_mdoc_file(self, mdoc_path: Path) -> Dict[str, Any]:
        """
        Fully parse an mdoc file into headers and data sections.
        This logic is from the original project_service.py.
        """
        header_lines = []
        data_sections = []
        current_section = {}
        in_zvalue_section = False

        with open(mdoc_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                if line.startswith("[ZValue"):
                    if current_section:
                        data_sections.append(current_section)
                    current_section = {"ZValue": line.split("=")[1].strip().strip("]")}
                    in_zvalue_section = True
                elif in_zvalue_section and "=" in line:
                    key, value = [x.strip() for x in line.split("=", 1)]
                    current_section[key] = value
                elif not in_zvalue_section:
                    header_lines.append(line)

        if current_section:
            data_sections.append(current_section)

        return {"header": "\n".join(header_lines), "data": data_sections}

    def write_mdoc_file(self, mdoc_data: Dict[str, Any], output_path: Path):
        """
        Writes a parsed mdoc data structure back to a file.
        This logic is from the original project_service.py.
        """
        with open(output_path, "w") as f:
            f.write(mdoc_data["header"] + "\n")
            for section in mdoc_data["data"]:
                z_value = section.pop("ZValue", None)
                if z_value is not None:
                    f.write(f"[ZValue = {z_value}]\n")
                for key, value in section.items():
                    f.write(f"{key} = {value}\n")
                f.write("\n")


_mdoc_service_instance = None


@lru_cache()
def get_mdoc_service() -> MdocService:
    """Get or create the MdocService singleton"""
    global _mdoc_service_instance
    if _mdoc_service_instance is None:
        _mdoc_service_instance = MdocService()
    return _mdoc_service_instance