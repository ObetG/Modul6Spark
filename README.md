# 🧬 BioSpark Intelligence Platform

> **Modul 6 – Big Data | Apache Spark RDD & DataFrames**  
> Sistem analisis terdistribusi data biopsi patologi skala besar menggunakan Apache Spark, PySpark, dan Streamlit.

---

## 📋 Deskripsi Aplikasi

**BioSpark Analytics** adalah platform web interaktif berbasis **Apache Spark** yang mensimulasikan analisis data biopsi patologi medis skala besar (hingga 2 juta baris). Aplikasi ini dibangun sebagai tugas **Modul 6 Big Data** untuk mempraktikkan konsep:

- ⚡ **Apache Spark RDD** — transformasi fungsional terdistribusi
- 🗄️ **Spark SQL & DataFrames** — query dan agregasi skala besar
- 📊 **Streamlit** — dashboard web interaktif
- 💾 **Apache Parquet** — penyimpanan columnar berkinerja tinggi
- 🔍 **Catalyst Optimizer** — analisis execution plan Spark

### Fitur Utama

| Tab | Fitur |
|-----|-------|
| 📋 Data Explorer | Filter & eksplorasi sampel data biopsi sintetis |
| 🔤 RDD Word Count | Pipeline `flatMap → map → reduceByKey` terdistribusi |
| 🔬 Analisis Patologi | Rule engine + visualisasi agregasi medis |
| ⚡ Benchmark | Uji performa berbagai konfigurasi partisi & skala data |
| 🔍 Execution Plan | Tampilan physical plan & RDD lineage graph |
| 💾 Export Parquet | Simpan & baca hasil agregasi format Parquet |

---

## 🛠️ Prasyarat (Prerequisites)

Pastikan semua software berikut sudah terinstal sebelum menjalankan aplikasi:

### 1. Java JDK 17 (Wajib untuk Apache Spark)

Download dari: https://www.microsoft.com/openjdk  
Install ke path: `C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot`

Verifikasi instalasi:
```bash
java -version
```

### 2. Apache Hadoop (Winutils untuk Windows)

1. Download **winutils.exe** dari: https://github.com/cdarlint/winutils
2. Buat folder `C:\hadoop\bin\`
3. Letakkan file `winutils.exe` dan `hadoop.dll` ke `C:\hadoop\bin\`

Verifikasi:
```bash
C:\hadoop\bin\winutils.exe ls C:\
```

### 3. Python 3.13

Download dari: https://www.python.org/downloads/  
Pastikan Python sudah ditambahkan ke `PATH` saat instalasi.

Verifikasi:
```bash
python --version
```

---

## 📦 Instalasi Dependensi Python

### Langkah 1: Clone / Download Proyek

```bash
git clone <url-repo-ini>
cd modul6
```

Atau download ZIP lalu ekstrak ke folder `modul6`.

### Langkah 2: (Opsional) Buat Virtual Environment

```bash
python -m venv venv
venv\Scripts\activate
```

### Langkah 3: Install Library yang Dibutuhkan

```bash
pip install pyspark streamlit plotly pandas
```

Daftar library yang digunakan:

| Library | Versi Minimum | Fungsi |
|---------|--------------|--------|
| `pyspark` | 3.4+ | Engine komputasi terdistribusi |
| `streamlit` | 1.30+ | Framework dashboard web |
| `plotly` | 5.0+ | Visualisasi grafik interaktif |
| `pandas` | 2.0+ | Manipulasi data di sisi driver |

---

## 🚀 Cara Menjalankan Aplikasi

### Metode 1: Menggunakan File `.bat` (Paling Mudah — Windows)

> ⚠️ **Perhatian:** Sebelum menjalankan, sesuaikan path Python di dalam file `run_web.bat`:
> ```bat
> set PYSPARK_PYTHON=C:\Users\<NamaUser>\AppData\Local\Programs\Python\Python313\python.exe
> ```
> Ganti `<NamaUser>` dengan username Windows kamu.

Klik dua kali file **`run_web.bat`**, atau jalankan via Command Prompt:

```bash
run_web.bat
```

Aplikasi akan otomatis terbuka di browser pada: **http://localhost:8501**

---

### Metode 2: Jalankan Manual via Terminal

1. Buka **Command Prompt** atau **PowerShell** di folder proyek
2. Set environment variables:

```powershell
$env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot"
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "$env:HADOOP_HOME\bin;$env:JAVA_HOME\bin;$env:PATH"
$env:PYSPARK_PYTHON = (Get-Command python).Source
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
```

3. Jalankan aplikasi Streamlit:

```bash
streamlit run app.py
```

Buka browser dan akses: **http://localhost:8501**

---

### Metode 3: Menggunakan `py` launcher (Windows)

```bash
py -m streamlit run app.py
```

---

## ✅ Verifikasi Instalasi Spark (Opsional)

Sebelum menjalankan aplikasi utama, kamu bisa memastikan Spark berjalan dengan baik menggunakan script test:

```bash
python test_spark.py
```

Output yang diharapkan:
```
Importing PySpark...
Building SparkSession...
Spark Version: 3.x.x
Count: 10
SUCCESS!
```

Jika muncul **"SUCCESS!"** berarti instalasi Java, Hadoop, dan PySpark sudah benar. ✅

---

## 🗂️ Struktur Direktori

```
modul6/
│
├── app.py                    # Aplikasi utama Streamlit + Spark
├── test_spark.py             # Script verifikasi instalasi Spark
├── run_web.bat               # Launcher Windows (klik 2x untuk jalankan)
├── MODUL6_v2.ipynb           # Notebook Jupyter referensi modul
├── biopsy_summary_parquet/   # Output file Parquet (dibuat saat export)
└── README.md                 # Dokumentasi ini
```

---

## 🔧 Konfigurasi Lanjutan

### Mengubah Path Java/Hadoop

Jika kamu menginstal Java atau Hadoop di lokasi yang berbeda, ubah baris berikut di `app.py`:

```python
# Baris 9-10 di app.py
JAVA_DEFAULT = r"C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot"
HADOOP_DEFAULT = r"C:\hadoop"
```

### Konfigurasi Spark

Parameter Spark yang digunakan (dapat diubah di `app.py`):

| Parameter | Nilai Default | Keterangan |
|-----------|-------------|------------|
| `spark.driver.memory` | `4g` | Memori driver Spark |
| `spark.sql.shuffle.partitions` | `8` | Jumlah partisi saat shuffle |
| `spark.sql.adaptive.enabled` | `true` | Adaptive Query Execution |
| `spark.driver.bindAddress` | `127.0.0.1` | Bind address (penting untuk Windows) |

---

## ❓ Troubleshooting

### ❌ Error: `JAVA_HOME is not set`
Pastikan Java JDK 17 sudah terinstal dan path di `app.py` / `run_web.bat` sesuai.

### ❌ Error: `winutils.exe not found` atau `hadoop.dll not found`
Download `winutils.exe` dan `hadoop.dll` untuk versi Hadoop yang sesuai dan letakkan di `C:\hadoop\bin\`.

### ❌ Error: `Port 8501 is already in use`
Jalankan Streamlit di port lain:
```bash
streamlit run app.py --server.port 8502
```

### ❌ Error: `No module named 'pyspark'`
Pastikan sudah menjalankan:
```bash
pip install pyspark
```
Jika menggunakan virtual environment, pastikan sudah diaktifkan terlebih dahulu.

### ❌ Spark lambat / memakan banyak RAM
Kurangi ukuran dataset dari sidebar aplikasi (pilih 100k baris) dan kurangi jumlah partisi.

### ❌ Halaman blank / error saat pertama load
Tunggu beberapa detik — Spark memerlukan waktu untuk inisialisasi JVM. Ini normal saat pertama kali dijalankan.

---

## 📌 Catatan Penting

> **Windows Users:** Aplikasi ini dioptimalkan untuk Windows dengan konfigurasi `spark.driver.bindAddress=127.0.0.1` dan path `PYSPARK_PYTHON` yang eksplisit untuk menghindari konflik worker Python di lingkungan lokal.

> **RAM:** Disarankan minimal **8 GB RAM** untuk menjalankan dataset 1 juta baris ke atas tanpa kendala.

---

## 👤 Informasi Pengembang

| | |
|---|---|
| **Mata Kuliah** | Big Data |
| **Modul** | Modul 6 — Apache Spark Core & SQL |
| **Teknologi** | Apache Spark, PySpark, Streamlit, Plotly |
| **Platform** | Windows (lokal) |

---

<p align="center">Built with ⚡ Apache Spark & 🧬 Python</p>
