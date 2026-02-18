# services/mdoc_service.py
import glob
from pathlib import Path
import sys
import os
from typing import Dict, Any
from functools import lru_cache

class MdocService:
    """Singleton service for all .mdoc file interactions."""

    def get_autodetect_params(self, mdocs_glob: str) -> Dict[str, Any]:
        """
        Parse the first VALID mdoc file found by the glob.
        """
        mdoc_files = glob.glob(mdocs_glob)
        if not mdoc_files:
            return {}

        mdoc_path = None
        for p in mdoc_files:
            if os.path.isfile(p) and p.endswith('.mdoc'):
                mdoc_path = Path(p)
                break
        
        if not mdoc_path:
            return {}

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
                        if in_zvalue_section:
                            break  # First ZValue section is complete, stop here
                        in_zvalue_section = True
                    elif in_zvalue_section and "=" in line:
                        key, value = [x.strip() for x in line.split("=", 1)]
                        first_section[key] = value
                    elif not in_zvalue_section and "=" in line:
                        key, value = [x.strip() for x in line.split("=", 1)]
                        header_data[key] = value

            if "SerialEM" in header_data.get("", ""):
                result["acquisition_software"] = "SerialEM"
                if "Tilt axis angle" in header_data:
                    result["tilt_axis_angle"] = float(header_data["Tilt axis angle"])
            else:
                result["acquisition_software"] = "Tomo5"
                if "RotationAngle" in first_section:
                    result["tilt_axis_angle"] = abs(float(first_section["RotationAngle"]))

            if "PixelSpacing" in header_data:
                result["pixel_spacing"] = float(header_data["PixelSpacing"])
            elif "PixelSpacing" in first_section:
                result["pixel_spacing"] = float(first_section["PixelSpacing"])

            if "Voltage" in header_data:
                result["voltage"] = float(header_data["Voltage"])
            elif "Voltage" in first_section:
                result["voltage"] = float(first_section["Voltage"])

            if "ImageSize" in header_data:
                sizes = header_data["ImageSize"].split()
                if len(sizes) >= 2:
                    result["detector_dimensions"] = (int(sizes[0]), int(sizes[1]))
            elif "ImageSize" in first_section:
                sizes = first_section["ImageSize"].split()
                if len(sizes) >= 2:
                    result["detector_dimensions"] = (int(sizes[0]), int(sizes[1]))

            if "ExposureDose" in first_section:
                exposure_dose = float(first_section["ExposureDose"])
                result["dose_per_tilt"] = round(exposure_dose * 1.5, 2)
                result["frame_dose"] = exposure_dose
            elif "ExposureDose" in header_data:
                exposure_dose = float(header_data["ExposureDose"])
                result["dose_per_tilt"] = round(exposure_dose * 1.5, 2)
                result["frame_dose"] = exposure_dose

            if "Magnification" in first_section:
                result["nominal_magnification"] = int(first_section["Magnification"])
            elif "Magnification" in header_data:
                result["nominal_magnification"] = int(header_data["Magnification"])

            if "SpotSize" in first_section:
                result["spot_size"] = int(first_section["SpotSize"])
            elif "SpotSize" in header_data:
                result["spot_size"] = int(header_data["SpotSize"])

            if "Binning" in first_section:
                result["binning"] = int(first_section["Binning"])
            elif "Binning" in header_data:
                result["binning"] = int(header_data["Binning"])

            subframe_path = first_section.get("SubFramePath", "")
            if "_EER.eer" in subframe_path or ".eer" in subframe_path.lower():
                result["eer_fractions_per_frame"] = 32

            if result.get("acquisition_software") == "SerialEM":
                result["invert_tilt_angles"] = False
            else:
                result["invert_tilt_angles"] = True

            return result

        except Exception as e:
            print(f"[ERROR] MdocService failed to parse {mdoc_path}: {e}", file=sys.stderr)
            return {}

    def parse_all_mdoc_files(self, mdocs_glob: str) -> Dict[str, Any]:
        """
        Parse ALL mdoc files and return comprehensive statistics.
        """
        mdoc_files = glob.glob(mdocs_glob)
        if not mdoc_files:
            return {}

        all_data = []
        result = {
            "mdoc_files": [],
            "tilt_series_count": 0,
            "total_tilts": 0,
            "tilt_range": (0, 0),
            "consistent_params": True
        }

        pixel_sizes = set()
        voltages = set()
        dose_rates = set()
        tilt_angles = []

        for mdoc_file in mdoc_files:
            if not os.path.isfile(mdoc_file): continue
            
            mdoc_path = Path(mdoc_file)
            try:
                parsed = self.parse_mdoc_file(mdoc_path)
                data_sections = parsed["data"]
                
                if data_sections:
                    # Extract parameters from first section of each file
                    first_section = data_sections[0]
                    
                    if "PixelSpacing" in first_section:
                        pixel_sizes.add(float(first_section["PixelSpacing"]))
                    if "Voltage" in first_section:
                        voltages.add(float(first_section["Voltage"]))
                    if "ExposureDose" in first_section:
                        dose_rates.add(float(first_section["ExposureDose"]))
                    
                    # Collect all tilt angles
                    for section in data_sections:
                        if "TiltAngle" in section:
                            tilt_angles.append(float(section["TiltAngle"]))
                    
                    result["total_tilts"] += len(data_sections)
                    result["mdoc_files"].append(mdoc_path.name)
                    
            except Exception as e:
                print(f"[WARN] Failed to parse {mdoc_file}: {e}")
                continue

        result["tilt_series_count"] = len(result["mdoc_files"])
        
        if tilt_angles:
            result["tilt_range"] = (min(tilt_angles), max(tilt_angles))
        
        result["consistent_params"] = (
            len(pixel_sizes) <= 1 and 
            len(voltages) <= 1 and 
            len(dose_rates) <= 1
        )
        
        if pixel_sizes:
            result["pixel_size"] = next(iter(pixel_sizes))
        if voltages:
            result["voltage"] = next(iter(voltages))
        if dose_rates:
            result["dose_per_frame"] = next(iter(dose_rates))
            result["dose_per_tilt"] = round(next(iter(dose_rates)) * 1.5, 2)  # Old logic

        return result

    def parse_mdoc_file(self, mdoc_path: Path) -> Dict[str, Any]:
        """
        Fully parse an mdoc file into headers and data sections.
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