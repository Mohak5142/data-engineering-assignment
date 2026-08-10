import pandas as pd
from pathlib import Path
ROOT=Path(__file__).parents[1]/'data'
def test_no_nulls_in_supplied_sources():
    for f in ROOT.glob('*.csv'):
        df=pd.read_csv(f)
        assert int(df.isna().sum().sum())==0, f"Nulls found in {f.name}"
def test_unique_primary_keys():
    for f,pk in [('patients.csv','patient_id'),('appointments.csv','appointment_id'),('billing.csv','bill_id'),('doctors.csv','doctor_id'),('treatments.csv','treatment_id')]:
        df=pd.read_csv(ROOT/f); assert df[pk].is_unique
