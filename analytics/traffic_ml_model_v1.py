import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import joblib

# Load data dari folder clean [cite: 80]
df = pd.read_csv('data/clean/traffic_smartcity_clean_v1.csv')
df['datetime'] = pd.to_datetime(df['datetime'])

# Feature Engineering sesuai modul [cite: 82, 83, 84]
df['hour'] = df['datetime'].dt.hour
df['day'] = df['datetime'].dt.dayofweek
df['lag1'] = df['traffic'].shift(1)
df = df.dropna()

X = df[['hour', 'day', 'lag1']]
y = df['traffic']

# Menggunakan model Random Forest [cite: 89]
model = RandomForestRegressor()
model.fit(X, y)

# Simpan hasil ke folder models [cite: 91]
joblib.dump(model, 'models/traffic_model_v1.pkl')
print("Model berhasil disimpan")
