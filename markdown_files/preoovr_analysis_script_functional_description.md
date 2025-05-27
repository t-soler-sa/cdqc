# Functional Doc Pre-Override Analysis

## Table of Contents

- [1. Input Data](#1input-data)
  - [1.1. Datasets](#11-datasets)
  - [1.2. Business Description of Datasets: what's in the dataset](#12-business-description-of-datasets-whats-in-the-dataset)
  - [1.3. Datasets Structure: Columns/Atrributes & Datatypes](#13-datasets-structure-columnsattribues--datatypes)

- [2. Processes](#2-processes)
  - [2.1. Override Activity](#21-override-activity)
    - [2.1.1 High-level Description / Summary](#211-high-level-description--summary)
  - [2.2. Pre-Override Analysis](#22-pre-override-analysis)
    - [2.2.1 High-level Description / Summary](#221-high-level-description--summary-1)

- [3. Output Data](#3-output-data)
  - [3.1. Output Audit Tables](#31-output-audit-tables)
  - [3.2. Business Description of Audit Tables: what's in the Autdit Pre-Overrides Analysis Tables](#32-business-description-of-audit-tables-whats-in-the-autdit-pre-overrides-analysis-tables)
  - [3.3. Audit Tables Structure: Columns/Atrributes & Datatypes](#33-audit-tables-structure-columnsattribues--datatypes)


## 1.Input Data
### 1.1. Datasets
### 1.2. Business Description of Datasets: what's in the dataset
### 1.3. Datasets Structure: Columns/Atrributes & Datatypes 

## 2. Processes & Transformations 
### 2.1. Override Activity
#### 2.1.1. High-level Description / Summary

Before proceeding with finding the delta of exclusion and delta of inclusion, we need to analyse if the data points in our override columns are still relevant or not.

Namely, if the latest datum delivered by Clarity is equal to the datum saved/stored in the override column, we consider that this override is not relevant anymore, in other words **it is not active.** Thus, for instanance, if for a given issuer/company we had the value "EXCLUDED" in the column OVR_CONTROV_STR001, and in the new delivery from Clarity, the value in the column STR_001_S is also "EXCLUDED" we understand that override needs to be "deactivated".

In that case we would remove the

### 2.2. Pre-Override Analysis
#### 2.2.1. High-level Description / Summary

## 3. Output Data
### 3.1. Output Audit Tables
### 3.2. Business Description of Audit Tables: what's in the Autdit Pre-Overrides Analysis Tables
### 3.3. Audit Tables Structure: Columns/Atrributes & Datatypes 