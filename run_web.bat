@echo off
title BioSpark Analytics Web Platform
echo ====================================================
echo Starting BioSpark Analytics Web Platform...
echo ====================================================

set JAVA_HOME=C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot
set HADOOP_HOME=C:\hadoop
set PATH=%HADOOP_HOME%\bin;%JAVA_HOME%\bin;%PATH%

REM Penting: Beritahu Spark worker Python yang dipakai (perbaikan Windows)
set PYSPARK_PYTHON=C:\Users\Obet\AppData\Local\Programs\Python\Python313\python.exe
set PYSPARK_DRIVER_PYTHON=%PYSPARK_PYTHON%

py -m streamlit run app.py
pause
