import os
import sys
import time
import warnings
warnings.filterwarnings("ignore")
import pandas as pd

# Konfigurasi path Java dan Hadoop secara otomatis untuk Windows
JAVA_DEFAULT = r"C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot"
HADOOP_DEFAULT = r"C:\hadoop"

# KRITIS: Beritahu Spark JVM worker Python mana yang harus dipakai (perbaikan Windows RDD)
PYTHON_EXE = sys.executable  # Path python.exe yang aktif saat ini
os.environ["PYSPARK_PYTHON"] = PYTHON_EXE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXE

if os.path.exists(JAVA_DEFAULT) and "JAVA_HOME" not in os.environ:
    os.environ["JAVA_HOME"] = JAVA_DEFAULT

if os.path.exists(HADOOP_DEFAULT) and "HADOOP_HOME" not in os.environ:
    os.environ["HADOOP_HOME"] = HADOOP_DEFAULT

if "HADOOP_HOME" in os.environ and "JAVA_HOME" in os.environ:
    os.environ["PATH"] = (
        os.path.join(os.environ["HADOOP_HOME"], "bin")
        + os.pathsep
        + os.path.join(os.environ["JAVA_HOME"], "bin")
        + os.pathsep
        + os.environ.get("PATH", "")
    )

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="BioSpark Analytics | Big Data Biopsy Platform",
    page_icon="🧬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- MODERN CUSTOM CSS ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Plus Jakarta Sans', sans-serif;
    }
    
    .main-header {
        background: linear-gradient(135deg, #0f172a 0%, #1e1b4b 50%, #311042 100%);
        padding: 2.2rem 2.5rem;
        border-radius: 18px;
        margin-bottom: 2rem;
        border: 1px solid rgba(255, 255, 255, 0.08);
        box-shadow: 0 10px 30px -10px rgba(0, 0, 0, 0.5);
    }
    
    .main-header h1 {
        color: #f8fafc;
        font-weight: 800;
        font-size: 2.3rem;
        margin: 0;
        letter-spacing: -0.02em;
    }
    
    .main-header p {
        color: #94a3b8;
        font-size: 1.05rem;
        margin-top: 0.5rem;
        margin-bottom: 0;
    }
    
    .spark-badge {
        display: inline-flex;
        align-items: center;
        background: rgba(226, 91, 34, 0.15);
        color: #fb923c;
        border: 1px solid rgba(226, 91, 34, 0.35);
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
        margin-right: 8px;
    }
    
    .stat-card {
        background: #1e293b;
        border: 1px solid #334155;
        border-radius: 14px;
        padding: 1.2rem 1.5rem;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    
    .stat-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.3);
    }
    
    .stat-label {
        color: #94a3b8;
        font-size: 0.85rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    .stat-value {
        color: #f1f5f9;
        font-size: 1.8rem;
        font-weight: 700;
        margin-top: 0.2rem;
    }
    
    .stat-sub {
        color: #38bdf8;
        font-size: 0.8rem;
        font-weight: 500;
        margin-top: 0.3rem;
    }
    
    .code-box {
        font-family: 'JetBrains Mono', monospace;
        background: #090d16;
        border: 1px solid #1e293b;
        border-radius: 10px;
        padding: 1.2rem;
        color: #e2e8f0;
        font-size: 0.85rem;
        overflow-x: auto;
    }
</style>
""", unsafe_allow_html=True)

# --- SPARK SESSION SINGLETON ---
_SPARK_SESSION = None

def get_spark():
    global _SPARK_SESSION
    if _SPARK_SESSION is not None:
        return _SPARK_SESSION
        
    try:
        from pyspark.sql import SparkSession
        spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("BioSparkWebAnalytics")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.driver.memory", "4g")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        _SPARK_SESSION = spark
        return _SPARK_SESSION
    except Exception as e:
        st.error(f"Gagal menginisialisasi Apache Spark: {str(e)}")
        return None

# Inisialisasi Spark
spark = get_spark()

# --- SIDEBAR CONTROLS ---
with st.sidebar:
    st.image("https://spark.apache.org/images/spark-logo-rev.svg", width=140)
    st.markdown("### ⚙️ Konfigurasi Spark & Data")
    
    if spark:
        sc = spark.sparkContext
        st.success(f"**Spark Active** (v{spark.version})")
        with st.expander("ℹ️ Info Driver & Cluster", expanded=False):
            st.write(f"• **Master**: `{sc.master}`")
            st.write(f"• **Default Cores**: `{sc.defaultParallelism}`")
            st.write(f"• **Java Version**: `{os.environ.get('JAVA_HOME', 'Auto')}`")
    else:
        st.error("Apache Spark belum aktif. Periksa instalasi Java.")
        st.stop()

    st.markdown("---")
    st.markdown("#### 🎯 Parameter Dataset")
    
    data_scale_choice = st.selectbox(
        "Ukuran Dataset Sintetis:",
        options=[100_000, 500_000, 1_000_000, 2_000_000],
        index=0,
        format_func=lambda x: f"{x:,} baris ({x//1000}k)"
    )
    
    custom_partitions = st.slider(
        "Jumlah Partisi RDD/DataFrame:",
        min_value=2,
        max_value=32,
        value=max(8, sc.defaultParallelism * 2),
        step=2
    )

    shuffle_partitions = st.select_slider(
        "Spark SQL Shuffle Partitions:",
        options=[2, 4, 8, 16, 32],
        value=8
    )
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))

    regenerate_btn = st.button("🔄 Generate & Refresh Data", type="primary", use_container_width=True)

# --- NATIVE SPARK DATA GENERATOR ---
def create_biopsy_data(spk, num_records, partitions):
    from pyspark.sql import functions as F
    
    start_t = time.perf_counter()
    
    base_df = spk.range(
        start=0,
        end=num_records,
        numPartitions=partitions
    ).withColumnRenamed("id", "patient_id")
    
    b_df = (
        base_df
        .withColumn(
            "age",
            (F.pmod(F.col("patient_id") * 17, F.lit(63)) + 18).cast("int")
        )
        .withColumn(
            "tissue_type",
            F.when(F.pmod(F.col("patient_id"), 3) == 0, "breast")
             .when(F.pmod(F.col("patient_id"), 3) == 1, "lung")
             .otherwise("skin")
        )
        .withColumn(
            "finding",
            F.when(F.pmod(F.col("patient_id"), 10) < 7, "normal")
             .when(F.pmod(F.col("patient_id"), 10) < 9, "abnormal")
             .otherwise("suspicious")
        )
        .withColumn(
            "mitotic_count",
            F.pmod(F.col("patient_id") * 7, 12).cast("int")
        )
        .withColumn(
            "pathology_note",
            F.concat_ws(
                " ",
                F.col("finding"),
                F.lit("tissue sample"),
                F.when(
                    F.col("mitotic_count") >= 6,
                    F.lit("high mitotic activity")
                ).otherwise(F.lit("low mitotic activity")),
                F.lit("pathology review")
            )
        )
    )
    
    b_df = b_df.cache()
    tot = b_df.count()
    elap = time.perf_counter() - start_t
    
    return b_df, tot, elap

# State persistence in session_state (avoids Streamlit pickle issues on Spark DataFrames)
need_recompute = (
    "biopsy_df" not in st.session_state
    or regenerate_btn
    or st.session_state.get("cached_scale") != data_scale_choice
    or st.session_state.get("cached_partitions") != custom_partitions
)

if need_recompute:
    with st.spinner(f"Memproses {data_scale_choice:,} data patologi di Spark Engine..."):
        biopsy_df, total_records, gen_time = create_biopsy_data(spark, data_scale_choice, custom_partitions)
        st.session_state["biopsy_df"] = biopsy_df
        st.session_state["total_records"] = total_records
        st.session_state["gen_time"] = gen_time
        st.session_state["cached_scale"] = data_scale_choice
        st.session_state["cached_partitions"] = custom_partitions
else:
    biopsy_df = st.session_state["biopsy_df"]
    total_records = st.session_state["total_records"]
    gen_time = st.session_state["gen_time"]

# --- HEADER SECTION ---
st.markdown("""
<div class="main-header">
    <div style="display: flex; gap: 8px; margin-bottom: 8px;">
        <span class="spark-badge">⚡ Apache Spark Core & SQL</span>
        <span class="spark-badge" style="background: rgba(56, 189, 248, 0.15); color: #38bdf8; border-color: rgba(56, 189, 248, 0.35);">🧬 Biopsy Pathology Analytics</span>
        <span class="spark-badge" style="background: rgba(74, 222, 128, 0.15); color: #4ade80; border-color: rgba(74, 222, 128, 0.35);">🟢 Local High-Speed Cluster</span>
    </div>
    <h1>BioSpark Intelligence Platform</h1>
    <p>Sistem Analisis Terdistribusi Data Biopsi Patologi Skala Besar Menggunakan Apache Spark RDD & DataFrames</p>
</div>
""", unsafe_allow_html=True)

# Top Metrics Row
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f"""
    <div class="stat-card">
        <div class="stat-label">Total Data Biopsi</div>
        <div class="stat-value">{total_records:,}</div>
        <div class="stat-sub">Distributed Rows</div>
    </div>
    """, unsafe_allow_html=True)
with c2:
    st.markdown(f"""
    <div class="stat-card">
        <div class="stat-label">Jumlah Partisi RDD</div>
        <div class="stat-value">{biopsy_df.rdd.getNumPartitions()}</div>
        <div class="stat-sub">Parallel Processing Units</div>
    </div>
    """, unsafe_allow_html=True)
with c3:
    st.markdown(f"""
    <div class="stat-card">
        <div class="stat-label">Waktu Generasi & Cache</div>
        <div class="stat-value">{gen_time:.3f} s</div>
        <div class="stat-sub">In-Memory Materialization</div>
    </div>
    """, unsafe_allow_html=True)
with c4:
    st.markdown(f"""
    <div class="stat-card">
        <div class="stat-label">Shuffle Partitions</div>
        <div class="stat-value">{shuffle_partitions}</div>
        <div class="stat-sub">Exchange Level</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# --- NAVIGATION TABS ---
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📋 Data Explorer & Sampel",
    "🔤 RDD Word Count Terdistribusi",
    "🔬 Analisis Patologi & Rule Engine",
    "⚡ Benchmark Partisi & Skala",
    "🔍 Execution Plan & RDD Lineage",
    "💾 Export & Parquet Storage"
])

# ==========================================
# TAB 1: DATA EXPLORER & SAMPLE
# ==========================================
with tab1:
    st.markdown("### 📋 Eksplorasi Data Sintetis Patologi")
    st.write("Data di-generate secara deterministik pada partisi Spark, mensimulasikan jutaan data pasien biopsi medis.")
    
    f1, f2, f3 = st.columns([1, 1, 2])
    with f1:
        filter_tissue = st.multiselect(
            "Filter Tipe Jaringan (Tissue Type):",
            options=["breast", "lung", "skin"],
            default=["breast", "lung", "skin"]
        )
    with f2:
        filter_finding = st.multiselect(
            "Filter Temuan (Finding):",
            options=["normal", "abnormal", "suspicious"],
            default=["normal", "abnormal", "suspicious"]
        )
    with f3:
        sample_limit = st.slider("Jumlah Baris Sampel yang Ditampilkan:", 5, 50, 15)

    filtered_df = biopsy_df.filter(
        (biopsy_df.tissue_type.isin(filter_tissue)) & 
        (biopsy_df.finding.isin(filter_finding))
    )
    
    sample_pd = filtered_df.limit(sample_limit).toPandas()
    
    st.dataframe(
        sample_pd,
        use_container_width=True,
        column_config={
            "patient_id": st.column_config.NumberColumn("Patient ID", format="%d"),
            "age": st.column_config.NumberColumn("Usia", format="%d th"),
            "tissue_type": st.column_config.TextColumn("Tipe Jaringan"),
            "finding": st.column_config.TextColumn("Hasil Temuan"),
            "mitotic_count": st.column_config.ProgressColumn(
                "Mitotic Count (0-11)",
                min_value=0,
                max_value=12,
                format="%d"
            ),
            "pathology_note": st.column_config.TextColumn("Catatan Patologi", width="large")
        }
    )
    
    with st.expander("🔍 Lihat Skema Dataframe Spark (printSchema)", expanded=False):
        schema_json = biopsy_df.schema.jsonValue()
        st.json(schema_json)

# ==========================================
# TAB 2: RDD WORD COUNT
# ==========================================
with tab2:
    st.markdown("### 🔤 Distributed Word Count dengan Spark RDD")
    st.markdown("""
    Pipeline transformasi RDD fungsional:
    `notes_rdd.flatMap(split) -> map((word, 1)) -> reduceByKey(add) -> takeOrdered(15)`
    """)
    
    if st.button("🚀 Jalankan Word Count RDD Sekarang", type="primary"):
        with st.spinner("Mengeksekusi transformasi RDD pada jutaan baris..."):
            notes_rdd = biopsy_df.select("pathology_note").rdd.map(lambda r: r["pathology_note"])
            words_rdd = notes_rdd.flatMap(lambda note: note.lower().split())
            pairs_rdd = words_rdd.map(lambda w: (w, 1))
            
            start_wc = time.perf_counter()
            word_counts_rdd = pairs_rdd.reduceByKey(lambda a, b: a + b, numPartitions=custom_partitions)
            top_words = word_counts_rdd.takeOrdered(15, key=lambda item: (-item[1], item[0]))
            wc_time = time.perf_counter() - start_wc
            
            wc_df = pd.DataFrame(top_words, columns=["Kata", "Frekuensi"])
            wc_df["Persentase"] = (wc_df["Frekuensi"] / total_records * 100).round(1).astype(str) + "%"
            
            w_col1, w_col2 = st.columns([1, 1.4])
            with w_col1:
                st.success(f"⏱️ Waktu Eksekusi RDD: **{wc_time:.3f} detik**")
                st.write(f"Partisi RDD yang dipakai: **{words_rdd.getNumPartitions()} partisi**")
                st.dataframe(wc_df, use_container_width=True)
            
            with w_col2:
                fig = px.bar(
                    wc_df,
                    x="Frekuensi",
                    y="Kata",
                    orientation="h",
                    color="Frekuensi",
                    color_continuous_scale="Purples",
                    title="15 Frekuensi Kata Teratas pada Catatan Patologi"
                )
                fig.update_layout(yaxis={'categoryorder':'total ascending'}, height=450)
                st.plotly_chart(fig, use_container_width=True)

# ==========================================
# TAB 3: CLINICAL ANALYTICS & RULE ENGINE
# ==========================================
with tab3:
    st.markdown("### 🔬 Analisis Patologi Medis & Diagnostic Rules")
    st.info("Kriteria Flag Peninjauan: Status ditandai **Needs Further Review** jika temuan adalah `abnormal`/`suspicious` ATAU `mitotic_count >= 6`.")
    
    from pyspark.sql import functions as F
    
    analyzed_df = biopsy_df.withColumn(
        "review_status",
        F.when(
            (F.col("finding").isin("abnormal", "suspicious")) | (F.col("mitotic_count") >= 6),
            "Needs Further Review"
        ).otherwise("No Rule-Based Flag")
    )
    
    if "status_summary_pd" not in st.session_state or need_recompute:
        with st.spinner("Menghitung agregasi data di Spark Engine..."):
            status_summary_pd = (
                analyzed_df.groupBy("review_status")
                .agg(
                    F.count("*").alias("record_count"),
                    F.round(F.avg("age"), 1).alias("average_age"),
                    F.round(F.avg("mitotic_count"), 2).alias("average_mitotic_count")
                )
                .orderBy("review_status")
                .toPandas()
            )
            
            tissue_summary_pd = (
                analyzed_df.groupBy("tissue_type", "finding")
                .agg(
                    F.count("*").alias("record_count"),
                    F.round(F.avg("mitotic_count"), 2).alias("average_mitotic_count")
                )
                .orderBy("tissue_type", "finding")
                .toPandas()
            )
            
            age_summary_pd = (
                analyzed_df.withColumn(
                    "age_group",
                    F.when(F.col("age") < 30, "18-29")
                     .when(F.col("age") < 45, "30-44")
                     .when(F.col("age") < 60, "45-59")
                     .otherwise("60+")
                )
                .groupBy("age_group", "review_status")
                .count()
                .orderBy("age_group", "review_status")
                .toPandas()
            )
            st.session_state["status_summary_pd"] = status_summary_pd
            st.session_state["tissue_summary_pd"] = tissue_summary_pd
            st.session_state["age_summary_pd"] = age_summary_pd
    else:
        status_summary_pd = st.session_state["status_summary_pd"]
        tissue_summary_pd = st.session_state["tissue_summary_pd"]
        age_summary_pd = st.session_state["age_summary_pd"]
    
    # Visualisasi Row 1
    r1_col1, r1_col2 = st.columns([1, 1.2])
    with r1_col1:
        st.markdown("#### Ringkasan Kebutuhan Review Pasien")
        st.dataframe(status_summary_pd, use_container_width=True)
        
        pie_fig = px.pie(
            status_summary_pd,
            names="review_status",
            values="record_count",
            color="review_status",
            color_discrete_map={
                "Needs Further Review": "#f43f5e",
                "No Rule-Based Flag": "#10b981"
            },
            hole=0.45,
            title="Proporsi Hasil Skrining"
        )
        st.plotly_chart(pie_fig, use_container_width=True)
        
    with r1_col2:
        st.markdown("#### Distribusi Temuan per Jaringan (Tissue Type)")
        tissue_fig = px.bar(
            tissue_summary_pd,
            x="tissue_type",
            y="record_count",
            color="finding",
            barmode="group",
            color_discrete_map={
                "normal": "#10b981",
                "abnormal": "#f59e0b",
                "suspicious": "#ef4444"
            },
            title="Jumlah Kasus berdasarkan Tipe Jaringan & Hasil Temuan"
        )
        st.plotly_chart(tissue_fig, use_container_width=True)

    st.markdown("---")
    st.markdown("#### Analisis Berdasarkan Kelompok Usia Pasien")
    age_fig = px.bar(
        age_summary_pd,
        x="age_group",
        y="count",
        color="review_status",
        barmode="stack",
        color_discrete_map={
            "Needs Further Review": "#f43f5e",
            "No Rule-Based Flag": "#0284c7"
        },
        title="Distribusi Status Review per Rentang Usia"
    )
    st.plotly_chart(age_fig, use_container_width=True)

# ==========================================
# TAB 4: BENCHMARK
# ==========================================
with tab4:
    st.markdown("### ⚡ Benchmark Kinerja Apache Spark")
    st.write("Mengukur efisiensi komputasi terdistribusi pada berbagai konfigurasi partisi dan skala data.")
    
    bench_col1, bench_col2 = st.columns(2)
    
    with bench_col1:
        st.markdown("#### 1. Shuffle Partitions Benchmark")
        st.caption("Mengukur pengaruh jumlah shuffle partition (2, 4, 8, 16) terhadap durasi agregasi groupBy.")
        if st.button("Uji Partisi Shuffle"):
            partition_results = []
            progress_bar = st.progress(0)
            partitions_to_test = [2, 4, 8, 16]
            
            for idx, shuffle_parts in enumerate(partitions_to_test):
                spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_parts))
                start_p = time.perf_counter()
                res = biopsy_df.groupBy("tissue_type", "finding").agg(F.count("*").alias("total"))
                res.collect()
                dur = time.perf_counter() - start_p
                partition_results.append({"Partitions": str(shuffle_parts), "Execution Time (s)": round(dur, 4)})
                progress_bar.progress((idx + 1) / len(partitions_to_test))
            
            p_df = pd.DataFrame(partition_results)
            st.dataframe(p_df, use_container_width=True)
            
            fig_p = px.line(
                p_df,
                x="Partitions",
                y="Execution Time (s)",
                markers=True,
                title="Waktu Eksekusi vs Jumlah Partisi Shuffle"
            )
            st.plotly_chart(fig_p, use_container_width=True)

    with bench_col2:
        st.markdown("#### 2. Data Scale Benchmark")
        st.caption("Menguji skalabilitas Spark pada 100k, 500k, hingga 1 juta baris.")
        if st.button("Uji Skala Data (100k - 1M)"):
            scale_results = []
            progress_bar2 = st.progress(0)
            scales = [100_000, 500_000, 1_000_000]
            
            for idx, n in enumerate(scales):
                test_df = (
                    spark.range(0, n, numPartitions=custom_partitions)
                    .withColumn("tissue_type", F.when(F.pmod(F.col("id"), 3) == 0, "breast").when(F.pmod(F.col("id"), 3) == 1, "lung").otherwise("skin"))
                    .withColumn("finding", F.when(F.pmod(F.col("id"), 10) < 7, "normal").when(F.pmod(F.col("id"), 10) < 9, "abnormal").otherwise("suspicious"))
                )
                start_s = time.perf_counter()
                test_df.groupBy("tissue_type", "finding").count().collect()
                dur_s = time.perf_counter() - start_s
                scale_results.append({"Records": f"{n:,}", "Duration (s)": round(dur_s, 4)})
                progress_bar2.progress((idx + 1) / len(scales))
            
            s_df = pd.DataFrame(scale_results)
            st.dataframe(s_df, use_container_width=True)
            
            fig_s = px.bar(
                s_df,
                x="Records",
                y="Duration (s)",
                color="Duration (s)",
                title="Waktu Komputasi berdasarkan Ukuran Data"
            )
            st.plotly_chart(fig_s, use_container_width=True)

# ==========================================
# TAB 5: EXECUTION PLAN & LINEAGE
# ==========================================
with tab5:
    st.markdown("### 🔍 Catalyst Optimizer & RDD Lineage Inspector")
    st.write("Analisis internal bagaimana Spark SQL Catalyst Engine mengoptimasi rencana eksekusi dan dependency graf.")
    
    col_plan1, col_plan2 = st.columns(2)
    
    with col_plan1:
        st.markdown("#### DataFrame Physical Execution Plan")
        try:
            import io
            from contextlib import redirect_stdout
            f = io.StringIO()
            with redirect_stdout(f):
                biopsy_df.groupBy("tissue_type", "finding").count().explain(mode="formatted")
            plan_str = f.getvalue()
        except Exception as err:
            plan_str = f"Execution Plan Error: {err}"
            
        st.markdown(f'<div class="code-box">{plan_str}</div>', unsafe_allow_html=True)
    
    with col_plan2:
        st.markdown("#### RDD Lineage Graph (`toDebugString`)")
        notes_rdd_sample = biopsy_df.select("pathology_note").rdd.map(lambda r: r["pathology_note"])
        words_rdd_sample = notes_rdd_sample.flatMap(lambda n: n.lower().split()).map(lambda w: (w, 1))
        word_counts_rdd_sample = words_rdd_sample.reduceByKey(lambda a, b: a + b, numPartitions=custom_partitions)
        
        lineage_str = word_counts_rdd_sample.toDebugString().decode("utf-8")
        st.markdown(f'<div class="code-box">{lineage_str}</div>', unsafe_allow_html=True)

# ==========================================
# TAB 6: PARQUET STORAGE & EXPORT
# ==========================================
with tab6:
    st.markdown("### 💾 Ekspor Hasil Agregasi ke Apache Parquet")
    st.write("Parquet adalah format penyimpanan columnar berkinerja tinggi dengan kompresi data efisien yang banyak digunakan dalam arsitektur Big Data.")
    
    output_folder = os.path.join(os.path.abspath("."), "biopsy_summary_parquet")
    st.code(f"Output Directory: {output_folder}")
    
    p_col1, p_col2 = st.columns(2)
    with p_col1:
        if st.button("💾 Simpan Ringkasan ke Parquet", type="primary"):
            try:
                start_p = time.perf_counter()
                (
                    tissue_summary_pd_spark := biopsy_df.groupBy("tissue_type", "finding")
                    .agg(
                        F.count("*").alias("record_count"),
                        F.avg("mitotic_count").alias("average_mitotic_count")
                    )
                )
                tissue_summary_pd_spark.write.mode("overwrite").parquet(output_folder)
                dur_p = time.perf_counter() - start_p
                st.success(f"✅ Data berhasil disimpan dalam format Parquet ({dur_p:.3f} s)")
            except Exception as e:
                st.error(f"Gagal menyimpan ke Parquet: {str(e)}")
    
    with p_col2:
        if st.button("📂 Baca Kembali File Parquet"):
            if os.path.exists(output_folder):
                try:
                    read_df = spark.read.parquet(output_folder)
                    st.dataframe(read_df.toPandas(), use_container_width=True)
                    st.info(f"Total baris yang terbaca: **{read_df.count()} baris**")
                except Exception as e:
                    st.error(f"Gagal membaca file Parquet: {str(e)}")
            else:
                st.warning("Folder Parquet belum dibuat. Klik 'Simpan Ringkasan ke Parquet' terlebih dahulu.")
