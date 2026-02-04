import requests
import pandas as pd
from app.database import mongo_collection, engine, SessionLocal
from app.models.personajes_sql import Base, Personaje
from sqlalchemy import text


# ---------- ENDPOINT A: EXTRACT ----------
def extract_characters_service(cantidad: int):
    url = "https://rickandmortyapi.com/api/character"
    params = {"page": 1}
    total_guardados = 0

    while total_guardados < cantidad:
        response = requests.get(url, params=params)
        data = response.json()
        results = data.get("results", [])

        for character in results:
            if total_guardados >= cantidad:
                break

            character_id = character["id"]

            # Idempotencia: evitar duplicados
            if mongo_collection.find_one({"id": character_id}):
                continue

            mongo_collection.insert_one(character)
            total_guardados += 1

        if data["info"]["next"] is None:
            break

        params["page"] += 1

    return {
        "mensaje": "Datos extraídos exitosamente",
        "registros_guardados": total_guardados,
        "fuente": "Rick & Morty API",
        "status": 201
    }

### Se crea la funcion transform_load_service ###


def transform_load_service():
    # 1. Se extraen los datos desde MongoDB
    documents = list(mongo_collection.find({}, {"_id": 0}))

    if not documents:
        return {
            "mensaje": "No hay datos para procesar",
            "registros_procesados": 0,
            "tabla_destino": "personajes_master",
            "status": 200
        }

    # 2. Se transforman con Pandas
    df = pd.json_normalize(documents)

    # Se seleccionan y renombran columnas
    df = df[[
        "id",
        "name",
        "status",
        "species",
        "gender",
        "origin.name",
        "location.name",
        "image"
    ]]

    df = df.rename(columns={
        "id": "id_personaje",
        "name": "nombre",
        "status": "estado",
        "species": "especie",
        "gender": "genero",
        "origin.name": "origen",
        "location.name": "ubicacion",
        "image": "imagen"
    })

    # Manejo de datos nulos
    df = df.fillna("N/A")

    # 3. Se carga a MySQL
    Base.metadata.create_all(bind=engine)

    registros_insertados = 0

    with engine.begin() as connection:
        for _, row in df.iterrows():
            insert_stmt = Personaje.__table__.insert().prefix_with("IGNORE").values(
                id_personaje=int(row["id_personaje"]),
                nombre=row["nombre"],
                estado=row["estado"],
                especie=row["especie"],
                genero=row["genero"],
                origen=row["origen"],
                ubicacion=row["ubicacion"],
                imagen=row["imagen"]
            )
            result = connection.execute(insert_stmt)
            registros_insertados += result.rowcount

    return {
        "mensaje": "Pipeline finalizado",
        "registros_procesados": registros_insertados,
        "tabla_destino": "personajes_master",
        "status": 200
    }

##Se crea la funcion reset_system_service ##
def reset_system_service():
    # MongoDB
    mongo_result = mongo_collection.delete_many({})
    mongo_deleted = mongo_result.deleted_count

    # MySQL
    db = SessionLocal()
    try:
        result = db.execute(text("DELETE FROM personajes_master"))
        mysql_deleted = result.rowcount
        db.commit()
    except Exception as e:
        db.rollback()
        raise e
    finally:
        db.close()

    return {
        "mensaje": "Sistema reseteado correctamente",
        "mongo_docs_eliminados": mongo_deleted,
        "mysql_rows_eliminadas": mysql_deleted,
        "status": 200
    }

 