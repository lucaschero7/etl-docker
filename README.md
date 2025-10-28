# Airflow Standalone (1 contenedor)

Pequeña guía de la estructura del proyecto y cómo trabajar con DAGs y código.

## Estructura de directorios

```
airflow-standalone/
├─ docker-compose.yml         # Define el servicio Airflow en modo standalone
├─ README.md                  # Este documento
├─ dags/                      # Archivos .py con DAGs y módulos simples de soporte
│  ├─ example_etl.py          # DAG de ejemplo (extract -> transform -> load)
│  └─ (tus_dags.py)           # Agrega aquí tus nuevos DAGs
├─ logs/                      # Logs de ejecución de tareas (persisten en tu host)
└─ plugins/                   # Extensiones de Airflow (operadores, hooks, sensores)
```

## Qué va en cada carpeta

- `dags/`
  - Todos los archivos `.py` con definiciones de DAG.
  - Puedes incluir módulos simples (carpetas con `__init__.py`) y luego importarlos desde tus DAGs.
  - Airflow añade esta ruta al `PYTHONPATH`, así que `from utils.io import load_csv` funciona si `dags/utils/io.py` existe.

- `plugins/`
  - Para componentes de Airflow reutilizables: `Operators` personalizados (heredan de `BaseOperator`), `Hooks`, `Sensors` y `Macros`.
  - No es necesario para comenzar; déjalo vacío hasta que lo necesites.

- `logs/`
  - Airflow guarda aquí los logs de cada tarea. Útil para debug y auditoría.

- `docker-compose.yml`
  - Ejecuta webserver + scheduler en un único contenedor (`command: standalone`).
  - Usa `SequentialExecutor` + SQLite (simple y sin dependencias).
  - Monta `./dags`, `./logs` y `./plugins` dentro del contenedor.

## Cómo agregar un nuevo DAG

1) Crea un archivo `.py` dentro de `dags/`, por ejemplo `dags/cotymania_daily.py`.
2) Define un `DAG` con `dag_id` único.
3) Guarda los cambios; Airflow recargará y lo mostrará en la UI.

Sugerencia para organizar código de soporte:

```
dags/
├─ cotymania/
│  ├─ __init__.py
│  ├─ tasks.py          # funciones que invocan tus operadores Python
│  └─ utils.py          # helpers de IO/transformación
└─ cotymania_dag.py     # DAG que importa desde cotymania/
```

Ejemplo de import dentro del DAG: `from cotymania.tasks import run_pipeline`.

## Logs y base de datos

- Los **logs** se guardan como archivos en `logs/` (no en la base de datos).
- La **metadata** (historial de DAGs, XComs, usuarios) en modo standalone está en SQLite dentro del contenedor (`/opt/airflow/airflow.db`). Si quieres persistirla entre recreaciones del contenedor, puedes montar esa ruta a una carpeta local (ver sección "Persistencia de metadata").

## Persistencia de metadata (opcional)

Si quieres conservar historial/usuarios al recrear el contenedor, puedes montar `/opt/airflow` (o solo el archivo DB) a una carpeta local. Por ejemplo, añadiendo a `airflow.volumes`:

```
- ./data:/opt/airflow
```

Ajusta si prefieres no duplicar `dags/logs/plugins` (puedes segmentar montajes según tu preferencia).

## Extender con código externo (opcional)

Si tienes bastante lógica de negocio y prefieres separarla de `dags/`, crea un `src/` y móntalo dentro del contenedor, añadiéndolo al `PYTHONPATH`:

```
volumes:
  - ./src:/opt/airflow/src
environment:
  - PYTHONPATH=/opt/airflow/src:/opt/airflow/dags
```

Luego podrás importar con `from myproject.etl.runner import run_job`.

## Comandos útiles

- Levantar: `docker compose up -d`
- Ver logs del servicio: `docker compose logs -f airflow`
- Entrar al contenedor: `docker compose exec airflow bash`
- Listar DAGs dentro del contenedor: `airflow dags list`

## Cuándo migrar a 2 contenedores (LocalExecutor + Postgres)

- Cuando necesites paralelismo real o mayor robustez de metadata.
- El cambio no requiere modificar tus DAGs, solo la configuración del `docker-compose.yml`.
