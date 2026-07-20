# 🚨 Sistema-de-Detecci-n-de-Fraude-Bancario-en-Tiempo-Real
________________________________________________________________________________________________________________________________________________________________________________________________________________
## ✦ Arquitectura de Autoridad Propia.✦

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

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()
________________________________________________________________________________________________________________________________________________________________________________________________________________
## 📬 Contáctame

![image](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)  https://www.linkedin.com/in/allan-gonzales-heredia-13a557b5/

![image](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)   https://github.com/AllGoHer

![image](https://img.shields.io/badge/Email-D14836?style=for-the-badge&logo=gmail&logoColor=white)   allgoher007@gmail.com

________________________________________________________________________________________________________________________________________________________________________________________________________________
**⭐ Muestra tu apoyo**

Si este proyecto te ha ayudado a aprender sobre ingeniería de datos en tiempo real, ¡dale un ⭐ en GitHub!
________________________________________________________________________________________________________________________________________________________________________________________________________________
Creado con ❤️ por <MARK>**ALLAN GONZALES**</MARK> | Ingeniero de datos | Especialista en datos en tiempo real
