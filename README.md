# CHN_Ultraviolet_MalignantMelanoma_Spatial_Analysis
A retrospective spatiotemporal analysis of environmental ultraviolet radiation exposure and malignant melanoma incidence in China at provincial and prefectural levels.

**Project Title: Spatiotemporal Association between Ambient UV Radiation and Malignant Melanoma in China**

**Background** This repository contains the data processing pipeline and spatial analysis code for a retrospective cohort study investigating the link between cumulative UV exposure and Melanoma subtypes (Acral vs. Cutaneous) across China.

**Data Sources**

- **Exposure Data**: Daily Erythemal UV Irradiance (10km grid) from _Jiang et al. (2024, Zenodo)_, aggregated to prefecture/province levels.
- **Geographic Data**: 2024 Standard Administrative Boundaries of China (Province & Prefecture levels).
    

**Methodology**

1. **Spatial Matching**: Point-in-polygon algorithm using Python (`geopandas`) to map 10km UV grid points to administrative units.
2. **Aggregation**: Calculation of annual mean, max, and cumulative exposure indices (2005-2020).
3. **Visualization**: Faceted choropleth mapping to demonstrate spatiotemporal heterogeneity.
    

**File Structure**

- `Code/`: Python scripts for data cleaning and GIS matching.
- `Result/`: Aggregated summary statistics (No PII).
- `Visualization/`: Trend plots and heatmaps.


**Maintainer** Jethro W. - Environmental Oncology Research