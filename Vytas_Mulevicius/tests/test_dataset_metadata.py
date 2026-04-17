import pytest
from lib.analysis.dataset_metadata import get_metadata, build_file_options, DatasetMetadata, KNOWN_METADATA


class TestGetMetadata:
    def test_known_file_jpsi(self):
        meta = get_metadata("Jpsimumu_Run2011A.csv")
        assert meta.mass_min == 2.0
        assert meta.mass_max == 5.0
        assert meta.expected_mass == 3.096
        assert meta.particle_name == "J/ψ"

    def test_known_file_zmumu(self):
        meta = get_metadata("Zmumu_Run2011A.csv")
        assert meta.mass_min == 70.0
        assert meta.mass_max == 110.0
        assert meta.expected_mass == 91.1876
        assert meta.particle_name == "Z"

    def test_known_file_upsilon(self):
        meta = get_metadata("Ymumu_Run2011A.csv")
        assert meta.expected_mass == 9.460
        assert meta.particle_name == "Υ"

    def test_known_file_w_boson(self):
        meta = get_metadata("Wenu.csv")
        assert meta.expected_mass == 80.38
        assert meta.particle_name == "W"

    def test_strips_directory_path(self):
        meta = get_metadata("data/Jpsimumu_Run2011A.csv")
        assert meta.particle_name == "J/ψ"

    def test_strips_url_query_string(self):
        meta = get_metadata("Jpsimumu_Run2011A.csv?token=abc123")
        assert meta.particle_name == "J/ψ"

    def test_strips_path_and_query_combined(self):
        meta = get_metadata("root://server//eos/Jpsimumu_Run2011A.csv?timeout=10")
        assert meta.particle_name == "J/ψ"

    def test_unknown_csv_returns_fallback_defaults(self):
        meta = get_metadata("MyParticle_Run2015.csv")
        assert meta.mass_min == 0.0
        assert meta.mass_max == 120.0
        assert meta.expected_mass == 0.0
        assert meta.particle_name == "MyParticle"

    def test_unknown_root_extracts_particle_name(self):
        meta = get_metadata("custom_data.root")
        assert meta.particle_name == "custom"

    def test_unknown_file_no_underscore(self):
        meta = get_metadata("events.csv")
        assert meta.particle_name == "events"

    def test_returns_dataset_metadata_instance(self):
        meta = get_metadata("Jpsimumu_Run2011A.csv")
        assert isinstance(meta, DatasetMetadata)

    def test_all_known_files_covered(self):
        for fname in KNOWN_METADATA:
            meta = get_metadata(fname)
            assert meta.expected_mass > 0
            assert meta.mass_max > meta.mass_min


class TestBuildFileOptions:
    def test_known_file_gets_display_label(self):
        opts, file_map = build_file_options(["Jpsimumu_Run2011A.csv"])
        assert opts[0] == "J/ψ → μμ (2011A)"
        assert file_map["J/ψ → μμ (2011A)"] == "Jpsimumu_Run2011A.csv"

    def test_unknown_file_gets_custom_label(self):
        opts, file_map = build_file_options(["custom_data.csv"])
        assert opts[0] == "📦 Custom: custom_data.csv"
        assert file_map["📦 Custom: custom_data.csv"] == "custom_data.csv"

    def test_mixed_known_and_unknown(self):
        opts, file_map = build_file_options(["Zmumu_Run2011A.csv", "custom.root"])
        assert len(opts) == 2
        assert "Z → μμ (2011A)" in opts
        assert "📦 Custom: custom.root" in opts

    def test_empty_input(self):
        opts, file_map = build_file_options([])
        assert opts == []
        assert file_map == {}

    def test_preserves_input_order(self):
        files = ["Zmumu_Run2011A.csv", "Jpsimumu_Run2011A.csv", "Ymumu_Run2011A.csv"]
        opts, _ = build_file_options(files)
        assert opts[0] == "Z → μμ (2011A)"
        assert opts[1] == "J/ψ → μμ (2011A)"
        assert opts[2] == "Υ → μμ (2011A)"

    def test_file_map_is_invertible(self):
        files = ["Zmumu_Run2011A.csv", "Wenu.csv"]
        opts, file_map = build_file_options(files)
        for label in opts:
            assert file_map[label] in files
