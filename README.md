# Setup Instructions

## Project Setup

- Clone the following repository:

```bash
git clone https://github.com/iupui-soic/handson-ml-mimic-iv
git checkout -b homl_mimic origin/homl_mimic
```

## Dataset Setup

Follow below steps to setup dataset after cloning the repository:

- Create a `dataset` directory in root directory.

- Download MIMIC-IV [dataset](https://physionet.org/content/mimiciv/1.0/) from after creating account inside `dataset` directory.

- Extact all the `.gz` files.

- Your dataset directory should look like this:

```bash
dataset/
├── core
│   ├── admissions.csv
│   ├── patients.csv
│   └── transfers.csv
├── hosp
│   ├── d_hcpcs.csv
│   ├── d_icd_diagnoses.csv
│   ├── d_icd_procedures.csv
│   ├── d_labitems.csv
│   ├── diagnoses_icd.csv
│   ├── drgcodes.csv
│   ├── emar.csv
│   ├── emar_detail.csv
│   ├── hcpcsevents.csv
│   ├── labevents.csv
│   ├── microbiologyevents.csv
│   ├── pharmacy.csv
│   ├── poe.csv
│   ├── poe_detail.csv
│   ├── prescriptions.csv
│   ├── procedures_icd.csv
│   └── services.csv
└── icu
    ├── chartevents.csv
    ├── d_items.csv
    ├── datetimeevents.csv
    ├── icustays.csv
    ├── inputevents.csv
    ├── outputevents.csv
    └── procedureevents.csv
```