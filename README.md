# 🚨 Sistema-de-Detecci-n-de-Fraude-Bancario-en-Tiempo-Real
________________________________________________________________________________________________________________________________________________________________________________________________________________
## ✦ Arquitectura de Autoría Propia.✦

***Un pipeline de Streaming con Apache Flink construido <mark>SIN JAVA</mark>. Potencia de Flink combinada con la simplicidad de <mark>Python</mark> y <mark>SQL</mark>.***
________________________________________________________________________________________________________________________________________________________________________________________________________________
![image](https://github.com/user-attachments/assets/635450ed-4a22-4267-b4aa-90dca6747fe7)
________________________________________________________________________________________________________________________________________________________________________________________________________________
### 📌 Nota de Arquitectura: El enfoque "Zero Java" en Apache Flink

Este proyecto fue diseñado, desarrollado y desplegado al 100% desde cero por mí, con una premisa de arquitectura clave: **Cero código Java**.

En la industria, Apache Flink es temido por su curva de aprendizaje base en Java/Scala (API DataStream). Este proyecto **rompe ese paradigma**, demostrando que se puede organizar un pipeline de Streaming de producción (1000 TPS, Event Time, Watermarks y Ventanas Temporales) utilizando exclusivamente Python para la ingesta/stateful generation y Flink SQL para el proceso completo.

El resultado es una arquitectura de datos moderna, altamente legible, mantenible y accesible para cualquier equipo de Data Engineering basado en Python/SQL, <mark>sin sacrificar el poder de cálculo de Flink</mark>. Demostrando así, que no necesitas un equipo de desarrolladores Java para mantener un sistema de detección de fraude en tiempo real de nivel empresarial; <mark>la lógica de negocio vive donde debe vivir: en SQL</mark>.

________________________________________________________________________________________________________________________________________________________________________________________________________________
![image](https://img.shields.io/badge/License-MIT-yellow.svg)
![image](https://img.shields.io/badge/Apache%2520Flink-1.17.1-blue)
![image](https://img.shields.io/badge/Apache%2520Kafka-3.9.0-black)
![image](https://img.shields.io/badge/MySQL-8.0-orange)
![image](https://img.shields.io/badge/Grafana-10.x-green)
![image](https://img.shields.io/badge/Docker-24.x-blue)
![image](https://github.com/user-attachments/assets/ef9e51f1-7111-4af0-bb7f-e8fcb270e86a)


**🎯 El reto: el fraude bancario en tiempo real**

*Cada segundo se producen miles de transacciones fraudulentas en todo el mundo. El procesamiento por lotes tradicional detecta el fraude horas más tarde, cuando el dinero ya se ha perdido*.

Este proyecto resuelve ese problema. Se trata de un canal de streaming de alta velocidad que detecta transacciones bancarias fraudulentas en tiempo real, utilizando Apache Flink como motor de procesamiento y Kafka como intermediario de eventos.

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🏗️ Arquitectura: canalización de streaming de extremo a extremo

![iamge](https://github.com/user-attachments/assets/b7205bcd-2f0f-4b99-b624-d8b2cb27a0f5)
![image](https://github.com/user-attachments/assets/43dc804e-ebed-45c9-96b5-1c3f220440ba)

________________________________________________________________________________________________________________________________________________________________________________________________________________
### 🔧 Análisis en profundidad de la arquitectura
________________________________________________________________________________________________________________________________________________________________________________________________________________
**1. Generación de datos (productor)**

* Simula más de 1000 transacciones por segundo

* Incorpora patrones de fraude:

  * Picos de velocidad: > 100 transacciones en 10 segundos por parte de un mismo usuario

  * Comportamiento de deriva: usuarios que cambian repentinamente de patrones

  * Indicadores directos de fraude: para probar la detección

* Utiliza 100 usuarios sintéticos con perfiles realistas

**2. Procesamiento de flujos (Flink)**

* Ventanas «tumbling»: ventanas de 10 segundos sin solapamiento

* Procesamiento con estado: realiza un seguimiento del comportamiento del usuario a lo largo del tiempo

* Reglas de detección de fraude:

  * Cualquier transacción con <mark>is_fraud=1</mark> → ALERTA

  * Cualquier usuario con > 100 transacciones/10 s → ALERTA

* **Semántica Exactly-Once**: garantiza la coherencia de los datos

**3. Almacenamiento y visualización**

* MySQL: almacena métricas agregadas y alertas

* Grafana: paneles en tiempo real con:

  * Alertas de detección de fraude en directo

  * Supervisión de la velocidad de las transacciones

  * Análisis del comportamiento de los usuarios

  * Tendencias históricas de fraude
________________________________________________________________________________________________________________________________________________________________________________________________________________
![image](https://github.com/user-attachments/assets/c06a0f98-9a74-45cc-8915-406f81cf784b)

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 📊 Indicadores clave de Desempeño (KPI)

| KPI | Descripción | Representación gráfica |
|-----|-------------|------------------------|
| 🚨 Alertas de fraude | Total de transacciones sospechosas detectadas | Big Number |
| ⚡Velocidad de transacciones | Número de transacciones por ventana de 10 segundos por usuario | Gauge Chart |
| 👤 Principales autores de fraude | Usuarios con más alertas de fraude | Bar Chart |
| 💰 Importe medio del fraude | Importe medio de las transacciones marcadas | Statistics |
| 📈 Tendencia del fraude | Detección de fraudes a lo largo del tiempo | Line Chart |

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🛠️ Pila tecnológica

| Capa | Tecnología | Finalidad |
|------|------------|-----------|
| Fuente de datos | Python + Kafka Producer | Simular transacciones en tiempo real |
| Broker de mensajes | Apache Kafka (KRaft) | Transmisión de eventos de alto rendimiento |
| Procesamiento | Apache Flink 1.17.1 | Procesamiento de flujos y detección de fraudes |
| Almacenamiento | MySQL 8.0 | Persistencia de métricas y alertas |
| Visualización | Grafana 10.x | Cuadros de mando en tiempo real |
| Orquestación | Docker Compose | Infraestructura en contenedores |

____________________________________________________________________________________________________________________________________
* **Ingesta (Productor):** Python 3.11 con . Utiliza múltiples hilos (multiproceso) y para transacciones similares legítimas y ataques de velocidad (Velocity Bursts).kafka-python-ngdataclasses.
* **Broker de Mensajes:** Apache Kafka 3.9.0 en modo KRaft (sin dependencias de Zookeeper).
* **Motor de Procesamiento:** Apache Flink 1.17.1. Se utiliza la API de Flink SQL para definir Marcas de agua, Hora del evento y ventanas Tumble de 10 segundos sin escribir código Java/Scala.
* **Alojamiento (Sink):** MySQL 8.0. (Decisión técnica: Se optó por MySQL sobre PostgreSQL/ClickHouse porque el conector JDBC nativo de Flink lo permite fuera de la caja en Windows, evitando errores de dialectos o dependencias faltantes).
* **Visualización:** Grafana configurada con actualización automática cada 5 segundos sobre consultas SQL directas a MySQL.
________________________________________________________________________________________________________________________________________________________________________________________________________________
![image](https://github.com/user-attachments/assets/5fc3c202-71f8-47e7-862d-85cff8d733a4)
________________________________________________________________________________________________________________________________________________________________________________________________________________
## 📚Estructura del Proyecto

![image](https://github.com/user-attachments/assets/cbd74f26-4484-4a40-936b-c984183e2211)

________________________________________________________________________________________________________________________________________________________________________________________________________________
## Guía de Implementación Paso a Paso
________________________________________________________________________________________________________________________________________________________________________________________________________________
**Paso 1: Configuración de la Infraestructura (Docker)**

Crear el archivo .

Notas críticas de la configuración:
[docker-compose.yml]()

1. Se usa <MARK>mysql_native_password</MARK> para evitar el error <MARK>Public Key Retrieval is not allowed</MARK> típico de MySQL 8.
   
2. Se exponen variables de entorno nativas de Flink (<MARK>FLINK_TASK_MANAGER_MEMORY</MARK>) en lugar de <MARK>FLINK_PROPERTIES</MARK> para evitar problemas de parsing en Windows CMD.
   
3. El puerto de MySQL se mapea al <MARK>3307</MARK> para no chocar con instalaciones locales.


**Paso 2: Construcción de la Imagen de Apache Flink**

Crear el archivo **flink/[Dockerfile]()**. Se añaden los conectores necesarios al classpath de Flink:

* **<MARK>flink-sql-connector-kafka</MARK>:** Para leer de Kafka.
* **<MARK>flink-connector-jdbc</MARK>:** Para escribir en bases de datos.
* **<MARK>mysql-connector-j</MARK>:** Driver JDBC de MySQL.

**Paso 3: El Simulador de Datos**

En **producer/[main.py]()** se implementó un generador que:

Usa <MARK>@dataclass</MARK> para estructuras de datos limpias y rápidas.
Implementa un patrón de "Deriva de Usuario" (Drift): Cada 500 transacciones, un usuario cambia su comportamiento normal a fraude masivo.
Configura <MARK>api_version=(2, 8, 0)</MARK> en el cliente de Kafka para evitar bloqueos de auto-descubrimiento en redes de Docker.

**Paso 4: Despliegue del Ecosistema**

Levantar la infraestructura desde la raíz del proyecto:

bash:

      docker compose up -d --build
      
Esperar a que los healthchecks de Kafka y MySQL estén en estado "healthy".

**Paso 5: Preparación de la Base de Datos Destino**

Flink requiere que la tabla destino exista antes de iniciar el Job. Se crea manualmente:

sql:

     CREATE TABLE fraud_metrics (
         window_start DATETIME,
         window_end DATETIME,
         user_id VARCHAR(255),
         tx_count BIGINT,
         total_amount DOUBLE,
         fraud_count BIGINT,
         is_fraud_alert BOOLEAN
     );

**Paso 6: Lanzamiento del Job de Procesamiento en Flink**

Se accede a la consola interactiva de Flink SQL:

bash:

      docker exec -it fraud_flink_jm /opt/flink/bin/sql-client.sh
      
Regla de oro de Windows: Para evitar que CMD elimine caracteres especiales como <MARK>&</MARK> o procese mal los <MARK>**;**</MARK>  , las sentencias SQL se pasaron formateadas en una sola línea por bloque.

1. **Tabla Origen (Kafka):** Se define el esquema, el Watermark (tolerancia de 5 segundos) y el conector.

2. **Tabla Destino (MySQL):** Se define el Sink JDBC. Importante: La URL queda limpia (<MARK>jdbc:mysql://mysql:3306/fraud_db</MARK>) gracias al paso de autenticación nativa.
   
3. **Ejecución:** Se lanza un <MARK>INSERT INTO ... SELECT</MARK> utilizando la función <MARK>TUMBLE</MARK> para agrupar los eventos en ventanas fijas de 10 segundos por usuario, calculando conteos, sumas y activando alertas si se superan los umbrales.


**Paso 7: Creación del Dashboard en Tiempo Real (Grafana)**

1. **Conexión**: Se configura la fuente de datos MySQL apuntando al host interno de Docker (<MARK>mysql:3306</MARK>).

2. **Paneles Creados**:

  * **KPIs (Formato Stat)**: Total de transacciones, Alertas de fraude (con fondo rojo condicional), Monto financiero en riesgo (formateado a USD).
    
  * **Serie Temporal (Time Series):** Trazabilidad del volumen de transacciones mapeando <MARK>window_start</MARK> al eje X.
  * **Tabla de Investigación (Table):** Log detallado de las ventanas que dispararon alertas, ordenado descendentemente.
  * **Streaming:** Se configuró el auto-refresh del dashboard cada <MARK>5s</MARK>.
_______________________________________________________________________________________________________________________________________________________________________________________________________________
## 📋Decisiones Técnicas y Solución de Problemas (Troubleshooting)
_______________________________________________________________________________________________________________________________________________________________________________________________________________
Como Data Engineer, durante el desarrollo se tomaron las siguientes decisiones para garantizar la estabilidad del pipeline en entornos locales (Windows/Docker Desktop):

**1. Problema de Memoria en Flink TaskManager:**

  * Síntoma: El TaskManager moría constantemente (<MARK>NoResourceAvailableException</MARK> o <MARK>Connection refused</MARK> en el puerto 6123).
  * Solución: Reducir la memoria estricta a <MARK>512m</MARK> y usar variables de entorno nativas de la imagen de Flink en lugar de parser manual de <MARK>FLINK_PROPERTIES</MARK>.
    
**2. Problema de Dialectos JDBC en Flink:**

  * Síntoma: Al intentar usar ClickHouse/Postgres, Flink lanzaba <MARK>No dialect found</MARK>.
  * Solución: Estandarizar en MySQL 8.0, cuyo dialecto está precompilado en el JAR oficial de <MARK>flink-connector-jdbc</MARK>.
    
**3. Problema de Parsing en Windows CMD:**

  * Síntoma: Las URLs de conexión con <MARK>&</MARK> (ej: <MARK>?useSSL=false&allowPublicKey...</MARK>) se rompían al pegarlas en la consola de Flink, causando fallos silenciosos de conexión.
  * Solución: Configurar MySQL con <MARK>mysql_native_password</MARK> para eliminar la necesidad de parámetros extra en la URL JDBC.

**4. Bloqueo del Productor de Kafka:**

  * Síntoma: El cliente de Python se quedaba colgado hasta 2 minutos al iniciar.
  * Solución: Forzar la versión de la API de Kafka <MARK>api_version=(2, 8, 0)</MARK> para evitar el handshake de descubrimiento automático.

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🧰Operación del Sistema
________________________________________________________________________________________________________________________________________________________________________________________________________________
* Levantar el proyecto.

  bash:

        docker compose up -d
  
* Ver logs del productor.

   bash:

        docker logs -f fraud_producer
  
* Verificar datos en DB.

  bash:

        docker exec -it fraud_mysql mysql -uadmin -padmin fraud_db -e "SELECT * FROM fraud_metrics ORDER BY window_start DESC LIMIT 10;"
  
* Acceder a Flink UI: http://localhost:8081 (Para verificar que el Job esté en estado RUNNING).
* Acceder a Grafana: http://localhost:3000 (admin / admin).
* Detener el proyecto.

  bash:

        docker compose down


________________________________________________________________________________________________________________________________________________________________________________________________________________
## 📬 Contáctame

![image](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)  https://www.linkedin.com/in/allan-gonzales-heredia-13a557b5/

![image](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)   https://github.com/AllGoHer

![image](https://img.shields.io/badge/Email-D14836?style=for-the-badge&logo=gmail&logoColor=white)   allgoher007@gmail.com

________________________________________________________________________________________________________________________________________________________________________________________________________________
**⭐ Muestra tu apoyo**

Si este proyecto te ha ayudado a aprender sobre ingeniería de datos en tiempo real, ¡dale un ⭐ en GitHub!
________________________________________________________________________________________________________________________________________________________________________________________________________________
### 🧠 Creado con ❤️ por <MARK>**ALLAN GONZALES**</MARK> | Ingeniero de datos | Especialista en datos en tiempo real

___________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🚀 DESARROLLO DEL PROYECTO
____________________________________________________________________________________________________________________________________________________________________________________________________________________________
1. Creamos una carpeta del proyecto (Fraud_Detection_V2) en la terminal

   bash: 

         mkdir Fraud_Detection_V2

Ahora creamos las subcarpetas.

**Opción 1. Git Bash (Recomendado)** ✅

Si estás en Git Bash (MINGW64), este comando funciona perfectamente.

Git bash:

           mkdir -p Fraud_Detection_V2/{producer,flink-sql,flink}


**Opción 2. PowerShell**

PowerShell no soporta la expansión con {}.

Debes hacer:

mkdir Fraud_Detection_V2

mkdir Fraud_Detection_V2\producer
mkdir Fraud_Detection_V2\flink-sql
mkdir Fraud_Detection_V2\flink

Resultado:

![image](https://github.com/user-attachments/assets/f32d02bb-e66b-45ad-b99f-7a93748ccef9)


2. Abrimos Visual Studio Code y lo vinculamos con la carpeta principal del proyecto (Fraud_Detection_V2) para crear los archivos con sus respectivos codigos.

2.1. [Docker-compose.yml]()

2.2. Producer/[producer.py](https://github.com/AllGoHer/Sistema-de-Detecci-n-de-Fraude-Bancario-en-Tiempo-Real/blob/main/producer/main.py)

2.2.1. 🧠 **Explicación del proposito de la creación del Producer**

Este código es un Productor Kafka de alto rendimiento. Está diseñado para simular tráfico bancario masivo inyectando patrones de fraude, pero sin colapsar la máquina ni saturar los logs.

1. Importaciones y Configuración Inicial

 python:

        import json
        import time
        import uuid
        import threading
        import logging
        import random 
        from dataclasses import dataclass, asdict
        from kafka import KafkaProducer


* **Módulos estándar**: <mark>json</mark> (serialización), <mark>time</mark> (control de tiempo y generación de timestamps), <mark>uuid</mark> (generación de IDs únicos universales), <mark>threading</mark> (concurrencia para lograr alto volumen), <mark>random</mark> (generación de montos variables).
  
* **<mark>dataclass y asdict</mark>**: Permite crear estructuras de datos limpias (como C# structs) sin escribir métodos <mark>__init__. asdict</mark> convierte ese objeto directamente en un diccionario de Python.
  
* **<mark>KafkaProducer</mark>**: El cliente oficial de Kafka para Python.


python:

        logging.basicConfig(level=logging.ERROR)
        logger = logging.getLogger(__name__)

* **Nivel ERROR**: Decisión de Senior. Si dejas el nivel por defecto (INFO o DEBUG), al imprimir 1000 transacciones por segundo, la consola de Windows o Docker se va a congelar. Silenciando todo excepto los errores, garantizas rendimiento puro.


python:

        KAFKA_BOOTSTRAP = "kafka:9092"
        TOPIC = "bank_transactions"
        TPS_PER_THREAD = 200 # 5 hilos * 200 = 1000 TPS
        
 * KAFKA_BOOTSTRAP: Usa kafka:9092 en lugar de localhost porque asume que corre dentro de una red de Docker.
 * TPS_PER_THREAD: Matemática de concurrencia. Para lograr 1000 Transacciones Por Segundo (TPS) sin matar el procesador, divide el trabajo en 5 hilos ligeros, cada uno haciendo 200 TPS.

**2. Modelo de Datos (La Estructura)**

python:

        @dataclass
        class Transaction:
            transaction_id: str
            user_id: str
            amount: float
            event_type: str
            is_fraud: int
            city: str
            device_type: str
            epoch_ms: int

* **Por qué <mark>dataclass</mark>**: En Streaming, la memoria y el rendimiento importan. Un <mark>dataclass</mark> es mucho más rápido y usa menos RAM que un diccionario normal (<mark>dict</mark>).
* **<mark>epoch_ms</mark>**: *Concepto crítico de Flink*. Guarda el tiempo en milisegundos Unix. En streaming NO puedes usar horas locales (como "2026-07-14 10:00:00") porque las zonas horarias y el reloj de la máquina rompen los Watermarks. El tiempo Unix es la única fuente absoluta de verdad.

**3. El Motor de Simulación (Lógica de Negocio)**

python:

        class FraudSimulator:
            def __init__(self):
                self.users = {f"USR-{i:05d}": {"city": "NYC", "drifting": False} for i in range(1, 101)}
                self.count = 0


* Crea 100 usuarios en memoria (<mark>USR-00001</mark> al <mark>USR-00100</mark>) usando dictionary comprehension.
* **<mark>self.count</mark>**: Un contador global para saber en qué "instante" del tiempo vamos.

python:

        def get_transaction(self):
            self.count += 1
            # Simulación de drift (Fraude) cada 500 transacciones
            is_drift = (self.count % 500) < 50 
            user_id = "USR-00001" if is_drift else f"USR-{(self.count % 100) + 1:05d}"

* **La línea más inteligente del código <mark>(self.count % 500) < 50</mark>**: Esto simula un "Concept Drift" (Deriva de Concepto). De cada 500 transacciones globales, 50 serán fraudulentas. Simula un ataque coordinado.
* **Asignación de Usuario:** Si es fraude (<mark>is_drift=True</mark>), siempre ataca el mismo usuario (<mark>USR-00001</mark>). Esto es vital para que las ventanas de Flink detecten el "Velocity Burst" (ráfaga de velocidad). Si es normal, distribuye las transacciones equitativamente entre los otros 100 usuarios usando módulo matemático.

python:

        return Transaction(
            transaction_id=str(uuid.uuid4()),
            user_id=user_id,
            amount=1500.0 if is_drift else round(random.uniform(10, 100), 2),
            event_type="VELOCITY_BURST" if is_drift else "PURCHASE",
            is_fraud=1 if is_drift else 0,
            city=self.users[user_id]["city"],
            device_type="Mobile App",
            epoch_ms=int(time.time() * 1000)
        )

* Genera el evento final. Si es drift, el monto es alto y fijo ($1500) y el tipo de evento cambia para que Flink lo clasifique distinto.

**4. El Hilo Productor (Concurrencia y Kafka)**

python:

        def producer_thread(simulator, thread_id):
            try:
                print(f"🔗 Hilo {thread_id} conectando a Kafka...")
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    api_version=(2, 8, 0), 
                    request_timeout_ms=5000,
                    value_serializer=lambda v: json.dumps(asdict(v)).encode('utf-8')
                )

* **<mark>try/except</mark>:** Si Kafka se cae, el hilo muere gracefulmente (con elegancia) sin crashear todo el script de Python.
* **<mark>api_version=(2, 8, 0)</mark>: Solución a un bug famoso de Python/Kafka en Docker. Si no pones esto, el cliente intenta "adivinar" la versión del broker haciendo ping de red, lo cual bloquea el hilo hasta por 2 minutos.
* **<mark>value_serializer=lambda v: ...</mark>:** Esta línea hace 3 cosas en milisegundos: Convierte el <mark>dataclass</mark> a diccionio (<mark>asdict(v)</mark>), lo pasa a string JSON (<mark>json.dumps</mark>) y lo convierte a bytes (<mark>encode('utf-8')</mark>), que es lo único que Kafka acepta.

python:

        while True:
            tx = simulator.get_transaction()
            producer.send(TOPIC, key=tx.user_id.encode('utf-8'), value=tx)
            time.sleep(1.0 / TPS_PER_THREAD)

* **Bucle infinito**: Corre para siempre (streaming continuo).
* **<mark>key=tx.user_id...</mark>**: Patrón Senior en Kafka. Al usar el <mark>user_id</mark> como "Key" de la partición, le garantizas a Flink que todos los eventos de <mark>USR-00001</mark> irán a la misma partición de Kafka y llegarán en orden estricto. Sin esto, Flink no podría hacer ventanas temporales correctamente.
* **<mark>time.sleep(1.0 / 200)</mark>**: Pausa de 0.005 segundos. Es el "throttler" (regulador) que frena al hilo para no enviar 1 millón de mensajes por segundo y colapsar la red.

  
**5. El Orquestador Principal (Main)**

python:

        if __name__ == "__main__":
            print("🚀 Iniciando Producer Senior (1000 TPS target)...")
            time.sleep(5) # Espera Kafka

* **<mark>time.sleep(5)</mark>**: Good Practice en contenedores. Cuando Docker levanta este script, Kafka aún no ha terminado de inicializar. Si intentas conectar antes, fallará. Esta pausa garantiza que el broker esté listo.


python:

        simulator = FraudSimulator()
        threads = [threading.Thread(target=producer_thread, args=(simulator, i), daemon=True) for i in range(5)]
    
        for t in threads: 
            t.start()

*Crea una sola instancia del simulador (<mark>FraudSimulator</mark>) y la comparte entre los 5 hilos. Esto permite que el contador <mark>self.count</mark> sea global y el patrón de "Drift" funcione correctamente sincronizado.
* **daemon=True**: Le dice a Python que estos hilos son "sirvientes". Si el proceso principal de Python se cierra (ej. con Ctrl+C), estos hilos se matan automáticamente, evitando procesos huérfanos en tu sistema operativo.

python:

        try:
            while True: time.sleep(1)
        except KeyboardInterrupt:
            print("🛑 Detenido.")

* Mantiene el hilo principal vivo infinitamente. El <mark>except KeyboardInterrupt</mark> captura el <mark>Ctrl+C</mark> para que puedas detener el programa limpiamente por consola.

2.3. producer/[Dockerfile](https://github.com/AllGoHer/Sistema-de-Detecci-n-de-Fraude-Bancario-en-Tiempo-Real/blob/main/producer/Dockerfile)

2.4. producer/[Requirements.txt](https://github.com/AllGoHer/Sistema-de-Detecci-n-de-Fraude-Bancario-en-Tiempo-Real/blob/main/producer/requirements.txt)

2.5. flink/[Dockerfile]()

2.6. flink-sql/[pipeline.sql]()

3. Abrimos el Docker Destktop y, empezamos la ejecucion del proyecto.

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🚀 EJECUCIÓN DEL PROYECTO
____________________________________________________________________________________________________________________________________________________________________________________________________________________________
**FASE 1: Levantamiento Limpio de Infraestructura**

Primero, limpiamos cualquier estado anterior para garantizar un despliegue desde cero, y levantamos la infraestructura compuesta por Kafka, MySQL, Flink y Grafana.

cmd:

     cd C:\Users\User\Fraud_Detection_V2
     docker compose down -v

<MARK>NOTA:</MARK> Espera a que termine de borrar.

cmd:

     docker compose up -d --build

<MARK>NOTA:</MARK>Espera a que termine de descargar/construir. No avances hasta que veas el prompt de nuevo.

![image](https://github.com/user-attachments/assets/993505ee-3fb8-4d66-a321-dfa387cbedc8)

🎥 https://youtu.be/4MBYGHRQPSY

* Comando de verificación:

cmd:

     docker compose ps

![image](https://github.com/user-attachments/assets/ae545c80-825a-4f8e-9edb-1e87b9f51d4a)

Como pueden ver, los healthchecks de Kafka y MySQL pasaron exitosamente. La infraestructura está lista.

🎥 https://youtu.be/TN3_Jx3wzD0

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
**FASE 2: Preparación de la Base de Datos Destino**

Apache Flink requiere que la tabla destino exista antes de lanzar el Job. Creamos la tabla en MySQL con los tipos de datos exactos que espera nuestra ventana temporal.

cmd:

     docker exec -it fraud_mysql mysql -uadmin -padmin fraud_db -e "CREATE TABLE fraud_metrics (window_start DATETIME, window_end DATETIME, user_id VARCHAR(255), tx_count BIGINT, total_amount DOUBLE, fraud_count BIGINT,      is_fraud_alert BOOLEAN);"


![image](https://github.com/user-attachments/assets/e67a255b-adc8-448f-9785-f2ea83701ae6)


 "El silencio en la consola significa éxito en DBA".

🎥 https://youtu.be/8tYI4vryhoE

____________________________________________________________________________________________________________________________________________________________________________________________________________________________
**FASE 3: Ingesta de Datos (El Productor)**

Abrimos una nueva ventana en nuestra terminal


Ahora arrancamos el productor de Python. Está configurado con 5 hilos para inyectar 1000 transacciones por segundo, incluyendo un patrón de 'concept drift' que simula ataques de velocidad cada 500 transacciones."

cmd:

     docker logs -f fraud_producer


![image](https://github.com/user-attachments/assets/36a51870-0f54-4fa4-a908-26a52575e31a)

🎥  https://youtu.be/B7lif0c-u_8

* Verificar que el producer está enviando datos a Kafka
  
bash:

      docker exec -it fraud_kafka bash -c "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bank_transactions --from-beginning --max-messages 10"


![image](https://github.com/user-attachments/assets/e44f0edf-babc-4401-a0ae-b775be8b21ee)

🎥 https://youtu.be/bWO5yenXcEE
____________________________________________________________________________________________________________________________________________________________________________________________________________________________
**FASE 4: Procesamiento en Tiempo Real (Apache Flink SQL)**

Abrimos una tercera ventana en la terminal.

Aquí es donde entra Apache Flink. Vamos a entrar a la consola SQL y definir nuestro pipeline de procesamiento. 

Por un tema de compatibilidad con el parser de Windows, utilizo sentencias formateadas en una sola línea.

**Paso 4.1: Entrar a Flink**

cmd:

     docker exec -it fraud_flink_jm /opt/flink/bin/sql-client.sh

![image](https://github.com/user-attachments/assets/e8a2fe7a-151c-4edd-9aeb-d9df5ab67249)


__________________________________________________________________________________________________________________________________________________________________

**Paso 4.2: Crear Tabla Origen Kafka**

sql:

     CREATE TABLE kafka_transactions (transaction_id STRING, user_id STRING, amount DOUBLE, event_type STRING, is_fraud INT, city STRING, device_type STRING, epoch_ms BIGINT, event_time AS TO_TIMESTAMP_LTZ(epoch_ms, 3),      WATERMARK FOR event_time AS event_time - INTERVAL '5' SECONDS) WITH ('connector' = 'kafka', 'topic' = 'bank_transactions', 'properties.bootstrap.servers' = 'kafka:9092', 'properties.group.id' = 'flink_video_demo',       'scan.startup.mode' = 'latest-offset', 'format' = 'json');


Aquí definimos el Watermark de 5 segundos y el Event Time basado en el epoch_milisegundos.

![image](https://github.com/user-attachments/assets/a2f92205-da2b-4731-8951-8f0bdef745ab)

🎥 https://youtu.be/CSQpx6z07xY

_______________________________________________________________________________________________________________________________________________________________________
**Paso 4.3: Crear Tabla Sink MySQL**

sql:

     CREATE TABLE fraud_metrics (window_start TIMESTAMP(3), window_end TIMESTAMP(3), user_id STRING, tx_count BIGINT, total_amount DOUBLE, fraud_count BIGINT, is_fraud_alert BOOLEAN) WITH ('connector' = 'jdbc', 'url' =       'jdbc:mysql://mysql:3306/fraud_db', 'table-name' = 'fraud_metrics', 'username' = 'admin', 'password' = 'admin', 'sink.buffer-flush.max-rows' = '100', 'sink.buffer-flush.interval' = '2s');


El Sink JDBC apunta a MySQL con un flush cada 2 segundos o cada 100 filas.

![image](https://github.com/user-attachments/assets/57d69ed3-1eb1-40e8-a774-6c53d14d0bf9)

🎥 https://youtu.be/NoHcCMSSX9M

___________________________________________________________________________________________________________________________________________________________________________

**Paso 4.4: Configurar Modo Detached**

sql:

     SET 'execution.attached' = 'false';

![image](https://github.com/user-attachments/assets/8837bff7-2e25-4e32-ae0a-fca3d790bbe6)

___________________________________________________________________________________________________________________________________________________________________________

**Paso 4.5: Lanzar el Job de Ventanas**

sql:

     INSERT INTO fraud_metrics SELECT window_start, window_end, user_id, COUNT(*) as tx_count, SUM(amount) as total_amount, SUM(is_fraud) as fraud_count, CASE WHEN SUM(is_fraud) > 100 OR COUNT(*) > 100 THEN TRUE ELSE         FALSE END as is_fraud_alert FROM TABLE(TUMBLE(TABLE kafka_transactions, DESCRIPTOR(event_time), INTERVAL '10' SECONDS)) GROUP BY window_start, window_end, user_id;

En cuanto presiones Enter, verás: [INFO] Submitting job with JobID: a1b2c3d4... Inmediatamente, cambia de pestaña en tu navegador y vé a Flink UI escribiendo localhost:8081. 

🎥 https://youtu.be/iT--MEDkOmg


![image](https://github.com/user-attachments/assets/bc2ab010-78f4-4de0-8a46-569ba3184f82)


![image](https://github.com/user-attachments/assets/181f02ab-c3e7-480a-8372-60ec8263a4a9)


El Job se envió al clúster y pueden ver aquí en la Web UI que el estado cambia instantáneamente a RUNNING con un paralelismo de 1.

🎥 https://youtu.be/kJTJNbauCJc


Luego, escribe <mark>QUIT;</mark> en la consola de Flink para salir limpiamente.

__________________________________________________________________________________________________________________________________________________________________________________________________________________________
**FASE 5: Validación de Persistencia (La Prueba de Fuego)**

Abrimos una cuarta pestaña en el terminal.


Para demostrar que el pipeline es end-to-end y no solo un proceso en memoria, vamos a consultar la base de datos directamente. Deben aparecer las ventanas cerradas de 10 segundos.

cmd:

     docker exec -it fraud_mysql mysql -uadmin -padmin fraud_db -e "SELECT * FROM fraud_metrics ORDER BY window_start DESC LIMIT 10;"


![image](https://github.com/user-attachments/assets/01799852-e78c-49cc-a256-228d15e33fa3)


Flink tomó los miles de eventos sueltos de Kafka, los agrupó en ventanas de 10 segundos por usuario, y las insertó limpiamente en MySQL.

🎥https://youtu.be/xd1eOFUEw1w

_________________________________________________________________________________________________________________________________________________________________________________________________________________________
**FASE 6: Visualización de Negocio (Grafana)**

Finalmente, exponemos estos datos a los equipos de negocio mediante un dashboard de streaming en Grafana.



En el navegador, en una pestaña nueva.

**Paso 1**.	Ve a http://localhost:3000 (Loguéate si es necesario).
   
**Paso 2**.	Conexión: Configuramos la conexión directa a MySQL usando el nombre interno del contenedor para evitar problemas de red.
   
  o	Connections -> Add data source -> MySQL.

  ![image](https://github.com/user-attachments/assets/cd0242c7-34fa-4204-a357-a5a1fcc94c6d)
  
  o	Host: mysql:3306
  
  o	Database: fraud_db
  
  o	User/Pass: admin / admin


  ![image](https://github.com/user-attachments/assets/8747dbe7-d753-415b-aa47-b7479737f512)

  
  o	Desplázate hacia abajo y haz Clic en Save & test. 

   ![image](https://github.com/user-attachments/assets/3eac2205-a5d6-494f-92fd-4493853509f3)

   ![image](https://github.com/user-attachments/assets/80ac9ca6-cd48-451e-8558-f53cb9023698)

________________________________________________________________________________________________________________________________________________________________________________________________
**Paso 3. Crear el Dashboard de Fraude**

Crear el Lienzo (Dashboard)


1.	En el menú izquierdo, haz clic en Dashboards.
   
2.	Arriba a la derecha, haz clic en el botón New Dashboard (Nuevo panel de control).
   
3.	Se abrirá una pantalla en blanco. Haz clic en el botón azul Add panel (Añadir panel).

Ahora estamos dentro del editor de paneles. Fíjate que abajo hay una pestaña que dice "Query". Ahí es donde va el código SQL.

![image](https://github.com/user-attachments/assets/d73309ee-e297-47ce-953d-f8a3ea7455d0)

_________________________________________________________________________________________________________________________________________
Ahora vamos a crear 7 KPIs

**1. 🚨 Alertas de Fraude (Big Number)**

Muestra el total de ventanas de 10 segundos que dispararon la alerta.

  * Formato (Arriba a la derecha): Stat
  * Color Mode (Izquierda): Background (Ponlo en Rojo intenso en Thresholds)

![image](https://github.com/user-attachments/assets/929bc8e7-314c-45e7-8742-2858f1230be9)


SQL:

     SELECT COUNT(*) FROM fraud_metrics WHERE is_fraud_alert = TRUE;

![image](https://github.com/user-attachments/assets/04685ae3-6f60-4b41-bde0-2eaa10460fd4)

________________________________________________________________________________________________________________________________________
     
**2. 👤 Usuarios Afectados (Big Number)**

Cuenta cuántos usuarios únicos han tenido un comportamiento anómalo.

  * Formato: Stat

SQL:

     SELECT COUNT(DISTINCT user_id) FROM fraud_metrics WHERE is_fraud_alert = TRUE;

 ![image](https://github.com/user-attachments/assets/f0ffcee2-cdc9-4600-bba5-4a51b8b761c9)

 _______________________________________________________________________________________________________________________________________

**3. 💰 Monto Promedio de Fraude (Big Number)**

Muestra el promedio de dinero movido en las ventanas que fueron marcadas como fraude.

  * Formato: Stat
  
SQL:

     SELECT AVG(total_amount) FROM fraud_metrics WHERE is_fraud_alert = TRUE;

![image](https://github.com/user-attachments/assets/8d7dd76e-e541-4c7f-b348-b284c25f0009)

__________________________________________________________________________________________________________________________________________________________
     
**4. ⚡ Velocidad Máxima (Gauge Chart)**

Este es espectacular. Muestra el récord máximo de transacciones acumuladas en una sola ventana de 10 segundos. Si pasa de 100, la aguja se pone en rojo.

  * Formato: Gauge
  
SQL:

     SELECT MAX(tx_count) FROM fraud_metrics;

![image](https://github.com/user-attachments/assets/b2d83150-0a99-4dcf-8aad-847ed3ebac74)

___________________________________________________________________________________________________________________________________________________________
     
**5. 📈 Tendencia de Fraudes (Line Chart)**

Muestra cómo evoluciona la cantidad de fraudes detectados en el tiempo (cada punto es una ventana de 10s).

Formato: Time series

<mark>Nota:</mark> Usamos UNIX_TIMESTAMP porque Grafana lo necesita para el eje X temporal

SQL:

     SELECT 
       UNIX_TIMESTAMP(window_start) as time_sec, 
       SUM(fraud_count) as value 
     FROM fraud_metrics 
     WHERE is_fraud_alert = TRUE 
     GROUP BY window_start 
     ORDER BY window_start ASC;

![image](https://github.com/user-attachments/assets/ee831523-4896-4dd0-828e-1921a4d78511)

__________________________________________________________________________________________________________________________________________________________

**6. 🎯 Top Usuarios Sospechosos (Bar Chart)**

Un ranking de los 5 usuarios que más fraudes han generado. Ideal para el equipo de seguridad.

Formato: Bar chart

SQL:

     SELECT 
       user_id as metric, 
       SUM(fraud_count) as value 
     FROM fraud_metrics 
     WHERE is_fraud_alert = TRUE 
     GROUP BY user_id 
     ORDER BY value DESC 
     LIMIT 5;

![image](https://github.com/user-attachments/assets/5625fa38-174e-4a7c-b884-024617e3063e)

___________________________________________________________________________________________________________________________________________________________________________________________________________

**7. 🔥 Distribución de Riesgo / Volumen (Pie Chart)**

Como no tenemos el fraud_type en la tabla final, haremos un Pie Chart que compare el "Pesos Muertos" (Ventanas limpias) contra las "Ventanas Críticas". A los reclutadores les encanta ver proporciones.

  * Formato: Pie chart

SQL:

     SELECT 
       CASE 
         WHEN is_fraud_alert = TRUE THEN '🚨 Ventanas Críticas' 
         ELSE '✅ Tráfico Normal' 
       END as metric, 
       COUNT(*) as value 
     FROM fraud_metrics 
     GROUP BY metric;
   
![image](https://github.com/user-attachments/assets/deaf581b-fe83-4321-a794-219064d943a1)

_____________________________________________________________________________________________________________________________________________________________________________________________________
**DASHBOARD FINAL**

![image](https://github.com/user-attachments/assets/d298dbb3-3165-427c-981a-33b79480057f)





