import uproot
import awkward as ak
import numpy as np

file_path = "data/00041834_00013937_1.dimuon.dst"

try:
    with uproot.open(file_path) as file:
        event_tree = file['Event;1']
        
        dimuon_node = event_tree['_Event_Dimuon_pPhys_Particles.']
        print(f"Dimuon object type: {type(dimuon_node)}")
        print(f"Dimuon interpretation: {dimuon_node.interpretation}")
        
        # Pull the raw data array
        data = dimuon_node.array(entry_stop=5)
        print("\nData type:", type(data))
        print("Awkward fields:", data.fields)
        print("First event raw structure:\n", data[0])
        
except Exception as e:
    print(f"Error parsing DST manually: {e}")
