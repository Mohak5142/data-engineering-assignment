import pandas as pd
from pathlib import Path
ROOT=Path(__file__).parents[1]/'data'
def test_no_show_rate():
    a=pd.read_csv(ROOT/'appointments.csv'); rate=(a.status.eq('No-show').mean()*100); assert round(rate,2)==26.00
def test_payment_success_proxy():
    b=pd.read_csv(ROOT/'billing.csv'); rate=(b.payment_status.eq('Paid').mean()*100); assert round(rate,2)==32.00
def test_treatment_average():
    t=pd.read_csv(ROOT/'treatments.csv'); assert round(t.cost.mean(),2)==2756.25
