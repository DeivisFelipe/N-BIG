# 📊 Network Traffic Analysis - Undergraduate Thesis

This repository is part of the Undergraduate Thesis (TCC) by **Deivis Felipe Guerreiro Fagundes**, presented to the Computer Science program at **Federal University of Santa Maria (UFSM)**, semester 2025/1.

## 🎯 Objective

The goal of this project is to analyze different types of network traffic using public datasets, which can be from **MAWI** or **CAIDA**. In this thesis, both datasets were used to enrich the analysis.

## 🗂️ Project Structure

- **PreProcessamento/**  
  Contains scripts to:

  - 🗄️ Make database backups.
  - 💾 Insert flows into the MongoDB database.
    > **Note:** To insert the flows, the PCAP file must be previously processed to generate a `.txt` file, where each line represents a flow extracted from the PCAP.
  - 🔗 Unify flows from different `.txt` files.  
    For this, the flows must have the same 5-tuple (source IP, source port, destination IP, destination port, protocol) and be within the timeout defined in the unification script.

  The unification process uses the code available at:  
  [https://github.com/DeivisFelipe/large-pcap-analyzer-2](https://github.com/DeivisFelipe/large-pcap-analyzer-2)  
  This project is capable of extracting flows from large PCAP files (including files of several gigabytes).

- **Processamento/**  
  Contains the Python script responsible for generating the charts.  
  It reads data from MongoDB and generates statistical charts, saving them in the `saida/{database_name}` folder.

## 🙏 Acknowledgments

Thank you for visiting this repository!  
This thesis was developed using both this project and [large-pcap-analyzer-2](https://github.com/DeivisFelipe/large-pcap-analyzer-2), which performs the unification of flows from PCAP files, in addition to the analyses and visualizations presented here.

---

## 📝 License

This project is **open source** and licensed under the MIT license. Feel free to use, modify, and contribute! 🚀
