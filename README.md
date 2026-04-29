# ResearchDB — ARMD-MGB Research Data Platform

> A web-based clinical research data management platform built for the 
> Antibiotic Resistance Microbiology Dataset Mass General Brigham (ARMD-MGB).

**Team 06** — William Lietz, Oguzhan Rejepov, Rachel Bell  
Middle Tennessee State University — CSCI 4560/5560, Spring 2026

---

## About

The ARMD-MGB dataset is a de-identified resource derived from electronic 
health records (EHR) that facilitates research in antimicrobial resistance 
(AMR). ResearchDB is a visual-first web application that allows researchers 
with little to no technical background to upload, explore, filter, and 
visualize millions of rows of clinical data through a browser — no SQL 
knowledge required.

### Supported Schema Types
| Schema | Description |
|---|---|
| Ward Encounters | Hospital admission records with ward type flags |
| Lab Results | Individual lab test results per patient |
| Area Deprivation Index | Neighborhood-level socioeconomic scores |
| Comorbidities | ICD-10 diagnosis codes per patient encounter |
| Demographics | Patient age and gender |
| Nursing Home Visits | Culture results from long-term care visits |

---

## Tech Stack

- **Backend:** Django 5.2, Django ORM
- **Frontend:** Bootstrap 5, Chart.js 4.4, DataTables 1.13
- **Database:** SQLite (development)
- **Data Processing:** pandas, openpyxl
- **Auth:** Django built-in authentication

---

## Setup Instructions

### Prerequisites
- Python 3.10+
- pip

### 1. Clone the repository
```bash
git clone https://github.com/0gu3han/DB_Research.git
cd DB_Research
```

### 2. Create and activate a virtual environment
```bash
python -m venv venv
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment variables
```bash
cp .env.example .env
```
Open `.env` and fill in the required values.

### 5. Run migrations
```bash
python manage.py migrate
```

### 6. Create a superuser (admin account)
```bash
python manage.py createsuperuser
```

### 7. Start the development server
```bash
python manage.py runserver
```

Visit `http://127.0.0.1:8000` in your browser.

---

## Usage

1. Register or log in
2. Navigate to **Upload Data**
3. Select your CSV file and choose the matching schema type
4. The system validates, parses, and loads your data automatically
5. Once status shows **Ready**, click **Explore** to browse, filter, and visualize
6. Use the **Export CSV** button to download filtered results

---

## Project Structure

---
DB_Research/
├── datasets/          # Sample/seed data files
├── researchdb/        # Main Django application
│   ├── models.py      # Database models (6 schema types)
│   ├── views.py       # All request handling logic
│   ├── urls.py        # URL routing
│   └── forms.py       # Upload and validation forms
├── static/            # CSS, JS, images
├── templates/         # HTML templates
├── manage.py
├── requirements.txt
└── .env.example

## Data Access Notice

The ARMD-MGB dataset is not included in this repository. Access requires 
formal training and a signed data use agreement through PhysioNet. 
See: https://doi.org/10.13026/2r5k-b955

---

## References

Z. Wei and S. Kanjilal, "Antibiotic Resistance Microbiology Dataset Mass 
General Brigham (ARMD-MGB)," PhysioNet, Dec. 2025. 
https://doi.org/10.13026/2r5k-b955
