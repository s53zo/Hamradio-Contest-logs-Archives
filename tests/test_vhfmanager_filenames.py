import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import download_vhfmanager_logs as vhfmanager  # noqa: E402


def test_safe_filename_component_replaces_windows_invalid_characters():
    assert vhfmanager.safe_filename_component("UW5Y*") == "UW5Y_"
    assert vhfmanager.safe_filename_component("S53M/P") == "S53M_P"
    assert vhfmanager.safe_filename_component('A<B>:C"D\\E|F?G') == "A_B_C_D_E_F_G"


def test_safe_filename_component_handles_windows_special_names_and_endings():
    assert vhfmanager.safe_filename_component("report. ") == "report"
    assert vhfmanager.safe_filename_component("CON.log") == "_CON.log"
    assert vhfmanager.safe_filename_component("...") == "_"
