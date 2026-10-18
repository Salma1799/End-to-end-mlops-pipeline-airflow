# Dashboard Streamlit construit SUR l'image Airflow déjà présente (pandas, pyarrow déjà installés)
# -> on ne télécharge que Streamlit + Plotly (quelques Mo), pas une nouvelle image complète.
FROM taxiflow-airflow:latest

RUN pip install --no-cache-dir streamlit==1.39.0 plotly==5.24.1

WORKDIR /app
COPY .streamlit /app/.streamlit
COPY app.py /app/app.py

EXPOSE 8501
ENTRYPOINT []
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--browser.gatherUsageStats=false"]
