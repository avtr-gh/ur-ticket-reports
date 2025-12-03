# URTicket

Script para obtener el CSV más reciente desde un bucket S3 y mostrar el primer registro.

Requisitos
- Python 3.8+
- Dependencias listadas en `requirements.txt` (boto3, python-dotenv)

Instalación (PowerShell)

```powershell
pip install -r requirements.txt
```

Uso

Coloca un archivo `.env` en la misma carpeta con al menos:

```
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_S3_BUCKET=nombre-del-bucket
AWS_DEFAULT_REGION=us-east-1
```

Ejecuta el script desde PowerShell para ver los logs:

```powershell
python .\app.py
```

Notas
- Si ejecutas el script haciendo doble-click en el Explorador de Windows, la consola se abrirá y cerrará rápidamente si ocurre un error; es mejor ejecutarlo desde una terminal para ver los mensajes.
- Si quieres que la ventana se quede abierta tras ejecutar desde doble-click, ejecuta `python -i app.py` o añade una variable de entorno `DEBUG_PAUSE=1` y modificación del script para pausar al final (no incluido por defecto).

Servicio HTTP para Cloud Run
----------------------------

He añadido `service.py` que expone dos endpoints:

- `GET /healthz` — health check (200 OK)
- `GET /latest` — devuelve JSON con metadata y el primer registro del CSV más reciente

Instalación local (PowerShell):

```powershell
pip install -r requirements.txt
python .\service.py
```

Prueba local (en otra consola PowerShell):

```powershell
Invoke-WebRequest -UseBasicParsing http://localhost:8080/latest | ConvertFrom-Json
```

Despliegue a Cloud Run (resumen):

1. Construir y subir la imagen (usando Cloud Build o Docker):

```powershell
gcloud builds submit --tag gcr.io/PROJECT-ID/urticket:latest
gcloud run deploy urticket --image gcr.io/PROJECT-ID/urticket:latest --region=REGION --platform=managed --allow-unauthenticated
```

2. En Cloud Run, configura variables de entorno o secretos (no incluyas `.env` en la imagen). Si sigues usando S3, guarda las credenciales en Secret Manager y pásalas como variables de entorno.

Seguridad importante:

- Rota las claves que estén en `.env` si están en un repositorio remoto.
- No subas `.env` al control de versiones — ya añadí `.env` a `.gitignore`.
