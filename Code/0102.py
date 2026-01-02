# ==============================================================================
# 项目：中国的紫外线数据收集
# 日期：2026-01-02
# ==============================================================================

# ---- Step 1 数据处理 ----
print(f"Step 1: 行政区划数据与UV数据整合")

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import os

# ==========================================
# 1. 配置路径与文件列表
# ==========================================
# Step 1: 读取行政区划数据 (Shapefile/GeoJSON)]
# 请将文件路径替换为您上传到 OSC 的实际路径
# 必须包含字段：省份名称 (Province), 城市名称 (City), 以及几何信息 (geometry)
# 建议使用 GeoJSON 格式，体积小且兼容性好

# 地理文件路径 (请根据实际位置修改)
GEO_PATH = "/users/PAS2978/wongzs/4. Bocheng/Ultraviolet_Melanoma/UV_Rawdata/CHN-Atlas/2. prefectures.json"  # 建议优先使用地级市数据
PROV_GEO_PATH = "/users/PAS2978/wongzs/4. Bocheng/Ultraviolet_Melanoma/UV_Rawdata/CHN-Atlas/1. provinces.json"  # 省级数据

# UV 数据文件列表
CSV_PATH = "/users/PAS2978/wongzs/4. Bocheng/Ultraviolet_Melanoma/UV_Rawdata/Ultraviolet(UV)/Zenodo-10 km Ultraviolet Radiation Product over mainland China 2005-2020/"
CSV_FILES = [
    "UV.2005-2008.average.csv",
    "UV.2009-2012.average.csv",
    "UV.2013-2016.average.csv",
    "UV.2017-2020.average.csv"
]

# 输出文件名
OUTPUT_FILE = "/users/PAS2978/wongzs/4. Bocheng/Ultraviolet_Melanoma/0102_CHN_UV_Atlas/Result/"

df = pd.read_csv(CSV_PATH+"UV.2005-2008.average.csv")
print(f"数据预览:\n{df.head()}")

# ==========================================
# 2. 加载地理底图 (只加载一次)
# ==========================================

print(f"[System] 正在加载地理数据: {GEO_PATH} ...")
try:
    gdf_admin = gpd.read_file(GEO_PATH)
    gdf_prov = gpd.read_file(PROV_GEO_PATH)
    # 确保坐标系为 WGS84
    if gdf_admin.crs != "EPSG:4326":
        gdf_admin = gdf_admin.to_crs("EPSG:4326")
    print(f"[System] 地理数据加载成功，共 {len(gdf_admin)} 个二级行政区划。")
    if gdf_prov.crs != "EPSG:4326":
        gdf_prov = gdf_prov.to_crs("EPSG:4326")
    print(f"[System] 地理数据加载成功，共 {len(gdf_prov)} 个一级行政区划。")
except Exception as e:
    print(f"[Error] 地理文件加载失败: {e}")
    exit()

# 准备一个列表存储每一步的处理结果
all_years_results = []
all_years_prov_results = [] # 在循环外初始化一个存储省份结果的列表

# ==========================================
# 3. 循环处理每个 CSV 文件
# ==========================================
for file_name in CSV_FILES:
#    print(f"文件地址为：{CSV_PATH+file_name}")
    if not os.path.exists(CSV_PATH+file_name):
        print(f"[Warning] 文件不存在，跳过: {file_name}")
        continue

    print(f"\n[Processing] 正在处理文件: {file_name} ...")
    
    # --- 3.1 读取 CSV ---
    # 仅读取必要的列，减少内存占用
    try:
        df = pd.read_csv(CSV_PATH+file_name)
        
        # 重命名复杂列名，方便操作
        df.rename(columns={
            'UV radiation (W m-2)': 'UV_Value',
            'longitude': 'lon',
            'latitude': 'lat'
        }, inplace=True)
        
    except Exception as e:
        print(f"[Error] 读取 CSV 失败: {e}")
        continue

    # --- 3.2 转换为地理数据 (GeoDataFrame) ---
    # 将经纬度转换为点几何对象
    geometry = [Point(xy) for xy in zip(df['lon'], df['lat'])]
    gdf_points = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    
    # 释放原始 dataframe 内存
    del df 

    # --- 3.3 空间连接 (Spatial Join) ---
    # 计算点在哪个多边形内 (inner join 丢弃落在国界外的点)
    print(f"   -> 正在进行空间匹配 ({len(gdf_points)} 个点)...")
    joined = gpd.sjoin(gdf_points, gdf_admin, how="inner", predicate="within")
    joined_prov = gpd.sjoin(gdf_points, gdf_prov, how="inner", predicate="within")

    # --- 3.4 聚合计算 (Aggregation) ---
    # 假设 json 中包含 'name' (城市名) 或 'adcode' (行政编码)
    # 这里的 'name' 需要根据您 prefectures.json 的实际列名修改，常见的是 'name', 'NAME', 'city_name'
    target_geo_col = 'name' # <--- [请确认此处] 如果您的json里城市名列名不是name，请修改这里
    target_prov_col = 'name' # 假设省份名称列也是 'name' (如果不是，请修改 target_prov_col)

    if target_geo_col not in joined.columns:
        print(f"[Error] JSON中找不到列名 '{target_geo_col}'。现有列名: {joined.columns.tolist()}")
        break

    # 按 城市 和 年份 分组计算均值
    # 注意：如果 CSV 里包含多一年的数据，我们需要保留 'year' 列进行分组
    print("   -> 正在按城市与年份聚合数据...")
    agg_result = joined.groupby([target_geo_col, 'year'])['UV_Value'].agg('mean').reset_index()
    agg_prov = joined_prov.groupby([target_prov_col, 'year'])['UV_Value'].agg('mean').reset_index()

    # 将这一批结果存入列表
    all_years_results.append(agg_result)
    all_years_prov_results.append(agg_prov)

    print(f"   -> 城市数据提取完成 {len(agg_result)} 条。")
    print(f"   -> 省份数据提取完成: {len(agg_prov)} 条。")

# ==========================================
# 4. 合并与保存
# ==========================================
if all_years_results:
    print("\n[System] 正在合并城市年份数据...")
    final_df = pd.concat(all_years_results, ignore_index=True)
    
    # 按城市和年份排序
    final_df.sort_values(by=[target_geo_col, 'year'], inplace=True)
    
    # 保存结果
    final_df.to_csv(OUTPUT_FILE+"01_China_City_Level_UV_Panel_2005_2020.csv", index=False, encoding='utf-8-sig') # sig 格式防止中文乱码
    print(f"[Success] 全部完成！结果已保存至: {OUTPUT_FILE}")
    print(f"数据预览:\n{final_df.head()}")
else:
    print("[Error] 未生成任何数据，请检查输入文件。")

if all_years_prov_results:
    print("\n[System] 正在合并省份年份数据...")
    final_prov_df = pd.concat(all_years_prov_results, ignore_index=True)
    final_prov_df.sort_values(by=['name', 'year'], inplace=True)
    
    # 保存为新文件 01_China_Province_Level...
    final_prov_df.to_csv(OUTPUT_FILE+"01_China_Province_Level_UV_Panel_2005_2020.csv", index=False, encoding='utf-8-sig')
    print(f"[Success] 省级结果已保存。")




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
