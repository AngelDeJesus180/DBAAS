# Contenedor de la "aplicación": Python + Jupyter para correr las pruebas
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8888
# Jupyter en http://localhost:8888  (token: logistica)
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", \
     "--IdentityProvider.token=logistica", "--ServerApp.root_dir=/app"]
