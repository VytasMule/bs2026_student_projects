from pydantic import BaseModel


class DatasetMetadata(BaseModel):
    """Physics metadata and default plot bounds for a known or inferred dataset.

    mass_min/mass_max are the default axis limits for the mass histogram, not
    hard physics cutoffs. expected_mass is the PDG value used for the reference line.
    """

    mass_min: float
    mass_max: float
    expected_mass: float
    particle_name: str


KNOWN_METADATA = {
    "Jpsimumu_Run2011A.csv": {"display": "J/ψ → μμ (2011A)", "min": 2.0, "max": 5.0, "mass": 3.096, "name": "J/ψ"},
    "Zmumu_Run2011A.csv": {"display": "Z → μμ (2011A)", "min": 70.0, "max": 110.0, "mass": 91.1876, "name": "Z"},
    "Ymumu_Run2011A.csv": {"display": "Υ → μμ (2011A)", "min": 8.0, "max": 12.0, "mass": 9.460, "name": "Υ"},
    "Wenu.csv": {"display": "W → eν (Electron)", "min": 0.0, "max": 120.0, "mass": 80.38, "name": "W"},
    "Wmunu.csv": {"display": "W → μν (Muon)", "min": 0.0, "max": 120.0, "mass": 80.38, "name": "W"},
}


def get_metadata(filename: str) -> DatasetMetadata:
    """Returns metadata for a dataset filename."""
    fname = filename.split('/')[-1].split('?')[0]
    if fname in KNOWN_METADATA:
        m = KNOWN_METADATA[fname]
        return DatasetMetadata(mass_min=m["min"], mass_max=m["max"], expected_mass=m["mass"], particle_name=m["name"])
    default_particle = fname.replace('.root', '').replace('.csv', '').split('_')[0]
    return DatasetMetadata(mass_min=0.0, mass_max=120.0, expected_mass=0.0, particle_name=default_particle)


def build_file_options(available_files: list[str]) -> tuple[list[str], dict[str, str]]:
    """Returns (display_options list, file_map dict) for the sidebar selectbox."""
    display_options = []
    file_map = {}
    for f in available_files:
        label = KNOWN_METADATA[f]["display"] if f in KNOWN_METADATA else f"📦 Custom: {f}"
        display_options.append(label)
        file_map[label] = f
    return display_options, file_map
