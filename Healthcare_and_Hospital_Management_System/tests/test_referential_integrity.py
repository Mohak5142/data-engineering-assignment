import pandas as pd
from pathlib import Path
ROOT=Path(__file__).parents[1]/'data'
def test_foreign_keys():
    p=pd.read_csv(ROOT/'patients.csv'); a=pd.read_csv(ROOT/'appointments.csv'); b=pd.read_csv(ROOT/'billing.csv'); d=pd.read_csv(ROOT/'doctors.csv'); t=pd.read_csv(ROOT/'treatments.csv')
    assert set(a.patient_id)<=set(p.patient_id)
    assert set(a.doctor_id)<=set(d.doctor_id)
    assert set(b.patient_id)<=set(p.patient_id)
    assert set(b.treatment_id)<=set(t.treatment_id)
    assert set(t.appointment_id)<=set(a.appointment_id)
