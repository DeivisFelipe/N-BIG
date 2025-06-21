# ⚙️ PreProcessing

This folder contains scripts for the preprocessing stage of the project.

## Main functionalities:
- 🗄️ Database backup for safety.
- 💾 Insertion of flows into the MongoDB database.
- 🔗 Unification of flows from different `.txt` files, as long as they have the same 5-tuple and are within the defined timeout.

> **Note:** To insert flows, the PCAP file must first be processed to generate a `.txt` file, where each line represents a flow.

The unification process uses the [large-pcap-analyzer-2](https://github.com/DeivisFelipe/large-pcap-analyzer-2) tool.

---
