from Configurables import DaVinci, PrintHeader, PrintDecayTree
from GaudiConf import IOHelper

# This script will simply load the first 5 events and print the DecayTree 
# which will help us identify the exact Stripping Line name present in the DIMUON stream.

DaVinci().InputType = 'DST'
DaVinci().DataType = '2012'
DaVinci().Simulation = False
DaVinci().EvtMax = 5
DaVinci().PrintFreq = 1

# Add standard printing tools to sequence
DaVinci().appendToMainSequence([PrintHeader()])

IOHelper().inputFiles(['data/00041834_00013937_1.dimuon.dst'])
