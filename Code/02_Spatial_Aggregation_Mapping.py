# ==============================================================================
# 项目：中国的紫外线数据收集
# 日期：2026-01-02
# ==============================================================================

setwd("/users/PAS2978/wongzs/4. Bocheng/Ultraviolet_Melanoma")

# ---- Step 2 UV 结果可视化 ----
print("Step 2: 基于行政区划数据的UV整合")


import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as ctx # 如果没有底图需求可忽略，但建议安装
import matplotlib.colors as mcolors

# ==========================================
# 1. 设置与数据加载
# ==========================================
# 载入之前生成的 CSV 结果
csv_path = "/users/PAS2978/wongzs/4. Bocheng/Ultraviolet_Melanoma/0102_CHN_UV_Atlas/Result/01_China_City_Level_UV_Panel_2005_2020.csv"
csv_path_prov = OUTPUT_FILE + "01_China_Province_Level_UV_Panel_2005_2020.csv"
df = pd.read_csv(csv_path)
df_prov = pd.read_csv(csv_path_prov)

# 载入地理底图 (必须与生成 CSV 时使用的底图一致，以保证 Name 匹配)
geo_path = "/users/PAS2978/wongzs/4. Bocheng/Ultraviolet_Melanoma/UV_Rawdata/CHN-Atlas/2. prefectures.json"  # 您的原始地理文件
gdf_map = gpd.read_file(geo_path)
gdf_map_prov = gpd.read_file(PROV_GEO_PATH) # 确保用 provinces.json

# 确保坐标系正确 (WGS84)
if gdf_map.crs != "EPSG:4326":
    gdf_map = gdf_map.to_crs("EPSG:4326")

# ==========================================
# 2. 数据融合 (Merge)
# ==========================================
# 注意：CSV 是长格式 (Long Format)，每个城市有 4 行数据
# 我们需要将地理信息合并到每一行
gdf_merged = gdf_map.merge(df, on='name', how='inner') # 'name' 必须一致
gdf_merged_prov = gdf_map_prov.merge(df_prov, on='name', how='inner')

# ==========================================
# 3.1 绘制分面地图 (市级-4个时间段)
# ==========================================
# 获取所有时间段的唯一值并排序
periods = sorted(df['year'].unique())

# 设置画板：2行2列
fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(16, 12))
axes = axes.flatten() # 将二维数组展平方便循环

# 统一色标范围 (关键！确保不同年份的颜色可比)
vmin = df['UV_Value'].min()
vmax = df['UV_Value'].max()

print(f"[Info] 绘图数值范围: Min={vmin:.1f}, Max={vmax:.1f}")

for i, period in enumerate(periods):
    ax = axes[i]
    
    # 筛选当前年份的数据
    data_subset = gdf_merged[gdf_merged['year'] == period]
    
    # 绘图
    data_subset.plot(
        column='UV_Value',
        ax=ax,
        legend=True,
        cmap='Spectral_r', # 倒谱色：红色代表高UV，蓝色代表低UV
        vmin=vmin, 
        vmax=vmax,
        edgecolor='black',
        linewidth=0.1,
        legend_kwds={'label': "UV Irradiance ($W/m^2$)", 'shrink': 0.6}
    )
    
    # 调整标题和样式
    ax.set_title(f"City-Level Mean UV Radiation: {period}", fontsize=12, fontweight='bold')
    ax.set_axis_off() # 去掉坐标轴

# ==========================================
# 4. 统计趋势图 (Boxplot) - 补充分析
# ==========================================
# 这一步是为了看全国整体 UV 是否在升高
fig_box, ax_box = plt.subplots(figsize=(10, 6))
df.boxplot(column='UV_Value', by='year', ax=ax_box, grid=False)
ax_box.set_title("National Trend of UV Radiation (2005-2020)")
ax_box.set_ylabel("UV Value ($W/m^2$)")
ax_box.set_xlabel("Time Period")
plt.suptitle("") # 去掉 pandas 自动生成的总标题

# ==========================================
# 5. 保存结果
# ==========================================
import os
output_dir = "/users/PAS2978/wongzs/4. Bocheng/Ultraviolet_Melanoma/0102_CHN_UV_Atlas/Result/"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

fig.savefig(f"{output_dir}02_Map_UV_Distribution_Panel_City_Level.png", dpi=300, bbox_inches='tight')
fig_box.savefig(f"{output_dir}/03_Trend_UV_Boxplot.png", dpi=300, bbox_inches='tight')

print(f"[Success] 图表已保存至 {output_dir} 文件夹。")

# ==========================================
# 3.2 绘制分面地图 (省级-4个时间段)
# ==========================================
# 获取所有时间段的唯一值并排序
periods = sorted(df_prov['year'].unique())

# 设置画板：2行2列
fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(16, 12))
axes = axes.flatten() # 将二维数组展平方便循环

# 统一色标范围 (关键！确保不同年份的颜色可比)
vmin = df_prov['UV_Value'].min()
vmax = df_prov['UV_Value'].max()

print(f"[Info] 绘图数值范围: Min={vmin:.1f}, Max={vmax:.1f}")

for i, period in enumerate(periods):
    ax = axes[i]
    
    # 筛选当前年份的数据
    data_subset = gdf_merged_prov[gdf_merged_prov['year'] == period]
    
    # 绘图
    data_subset.plot(
        column='UV_Value',
        ax=ax,
        legend=True,
        cmap='Spectral_r', # 倒谱色：红色代表高UV，蓝色代表低UV
        vmin=vmin, 
        vmax=vmax,
        edgecolor='black',
        linewidth=0.1,
        legend_kwds={'label': "UV Irradiance ($W/m^2$)", 'shrink': 0.6}
    )
    
    # 调整标题和样式
    ax.set_title(f"Province-Level Mean UV Radiation: {period}", fontsize=12, fontweight='bold')
    ax.set_axis_off() # 去掉坐标轴

fig.savefig(f"{output_dir}02_Map_UV_Distribution_Panel_Province_Level.png", dpi=300, bbox_inches='tight')
